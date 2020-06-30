/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.WrapperConfig;
import ash.nazg.spark.WrapperBase;
import ash.nazg.storage.Adapters;
import ash.nazg.storage.HadoopAdapter;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.OutputAdapter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DistWrapper extends WrapperBase {
    private final boolean local;
    protected DistCpSettings settings;
    private String codec;
    private boolean deleteOnSuccess = false;
    private Map<String, Tuple3<String[], String[], Character>> sinkInfo;

    public DistWrapper(JavaSparkContext context, WrapperConfig config, boolean local) {
        super(context, config);
        this.local = local;
        
        settings = DistCpSettings.fromConfig(wrapperConfig);
    }

    // from, to, group, ?sink
    private void distCpCmd(List<Tuple4<String, String, String, String>> list) {
        JavaRDD<Tuple4<String, String, String, String>> srcDestGroups = context.parallelize(list, list.size());

        // sink?, dest -> files
        Map<Tuple2<String, String>, List<String>> discoveredFiles = srcDestGroups
                .mapToPair(srcDestGroup -> {
                    List<String> files = new ArrayList<>();
                    try {
                        Path srcPath = new Path(srcDestGroup._1());

                        Configuration conf = new Configuration();

                        FileSystem srcFS = srcPath.getFileSystem(conf);
                        RemoteIterator<LocatedFileStatus> srcFiles = srcFS.listFiles(srcPath, true);

                        Pattern pattern = Pattern.compile(srcDestGroup._3());

                        while (srcFiles.hasNext()) {
                            String srcFile = srcFiles.next().getPath().toString();

                            Matcher m = pattern.matcher(srcFile);
                            if (m.matches()) {
                                files.add(srcFile);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Exception while enumerating files to copy: " + e.getMessage());
                        e.printStackTrace(System.err);
                        System.exit(13);
                    }

                    return new Tuple2<>(new Tuple2<>(srcDestGroup._4(), srcDestGroup._2()), files);
                })
                .combineByKey(t -> t, (c, t) -> {
                    c.addAll(t);
                    return c;
                }, (c1, c2) -> {
                    c1.addAll(c2);
                    return c1;
                })
                .collectAsMap();

        CopyFilesFunction cff = new CopyFilesFunction(deleteOnSuccess, codec, sinkInfo);

        SparkContext sc = context.sc();
        int numOfExecutors = local ? 1 : sc.statusTracker().getExecutorInfos().length - 1;

        List<Tuple3<List<String>, String, String>> regrouped = new ArrayList<>();

        for (Map.Entry<Tuple2<String, String>, List<String>> group : discoveredFiles.entrySet()) {
            int desiredNumber = numOfExecutors;

            String sink = group.getKey()._1;
            if (sink != null) {
                desiredNumber = wrapperConfig.inputParts(sink);
                if (desiredNumber <= 0) {
                    desiredNumber = numOfExecutors;
                }
            }

            List<String> sinkFiles = group.getValue();
            int countOfFiles = sinkFiles.size();

            int groupSize = countOfFiles / desiredNumber;
            if (groupSize <= 0) {
                groupSize = 1;
            }

            List<List<String>> sinkParts = Lists.partition(sinkFiles, groupSize);

            for (int i = 0; i < sinkParts.size(); i++) {
                List<String> sinkPart = sinkParts.get(i);

                regrouped.add(new Tuple3<>(new ArrayList<>(sinkPart), group.getKey()._2 + "/part-" + String.format("%05d", i), sink));
            }
        }

        context.parallelize(regrouped, regrouped.size())
                .foreach(cff);
    }

    public void go() {
        CpDirection distDirection = CpDirection.parse(wrapperConfig.getDistCpProperty("wrap", "nop"));
        if (distDirection == CpDirection.BOTH_DIRECTIONS) {
            throw new InvalidConfigValueException("DistWrapper's copy direction can't be 'both' because it's ambiguous");
        }

        if (distDirection.anyDirection && settings.anyDirection) {
            codec = wrapperConfig.getDistCpProperty("codec", "none");

            if (distDirection == CpDirection.FROM_CLUSTER) {
                deleteOnSuccess = Boolean.parseBoolean(wrapperConfig.getDistCpProperty("move", "true"));
            }

            if (distDirection.toCluster && settings.toCluster) {
                List<Tuple4<String, String, String, String>> inputs = new ArrayList<>();

                List<String> sinks = wrapperConfig.getInputSink();
                sinkInfo = new HashMap<>();
                for (String sink : sinks) {
                    String path = wrapperConfig.inputPath(sink);

                    InputAdapter inputAdapter = Adapters.input(path);
                    if (inputAdapter instanceof HadoopAdapter) {
                        sinkInfo.put(sink, new Tuple3<>(wrapperConfig.getSinkSchema(sink), wrapperConfig.getSinkColumns(sink), wrapperConfig.getSinkDelimiter(sink)));

                        System.out.println("Sink: " + sink);
                        System.out.println("- schema: " + Arrays.toString(wrapperConfig.getSinkSchema(sink)));
                        System.out.println("- columns: " + Arrays.toString(wrapperConfig.getSinkColumns(sink)));
                        System.out.println("- delimiter: " + wrapperConfig.getSinkDelimiter(sink));

                        List<Tuple3<String, String, String>> splits = DistCpSettings.srcDestGroup(path);
                        for (Tuple3<String, String, String> split : splits) {
                            inputs.add(new Tuple4<>(split._2(), settings.inputDir + "/" + sink, split._3(), sink));
                        }
                    }
                }

                distCpCmd(inputs);
            }

            if (distDirection.fromCluster && settings.fromCluster) {
                if (settings.wrapperStorePath != null) {
                    final String source = settings.wrapperStorePath + "/outputs/part-00000";
                    List<Tuple4<String, String, String, String>> outputs = context.wholeTextFiles(source.substring(0, source.lastIndexOf('/')))
                            .filter(t -> t._1.equals(source))
                            .flatMap(t -> {
                                String[] s = t._2.split("\\R+");
                                return Arrays.asList(s).iterator();
                            })
                            .collect().stream()
                            .map(output -> {
                                String path = String.valueOf(output);
                                String name = path.substring((settings.outputDir + "/").length());

                                return new Tuple4<>(path, wrapperConfig.outputPath(name), ".*/(" + name + ".*?)/part.*", (String) null);
                            })
                            .collect(Collectors.toList());

                    distCpCmd(outputs);
                } else {
                    List<String> tees = wrapperConfig.getTeeOutput();

                    List<Tuple4<String, String, String, String>> teeList = new ArrayList<>();
                    for (String tee : tees) {
                        if (tee.endsWith("*")) {
                            throw new InvalidConfigValueException("A call of configuration with wildcard task.tee.output must" +
                                    " have wrapper store path set");
                        }

                        String path = wrapperConfig.outputPath(tee);
                        OutputAdapter outputAdapter = Adapters.output(path);
                        if (outputAdapter instanceof HadoopAdapter) {
                            if (Adapters.PATH_PATTERN.matcher(path).matches()) {
                                teeList.add(new Tuple4<>(settings.outputDir + "/" + tee, path, ".*/(" + tee + ".*?)/part.*", null));
                            } else {
                                throw new InvalidConfigValueException("Output path '" + path + "' must point to a subdirectory for an output '" + tee + "'");
                            }
                        }
                    }

                    distCpCmd(teeList);
                }
            }
        }
    }
}
