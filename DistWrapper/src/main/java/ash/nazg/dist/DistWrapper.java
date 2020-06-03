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
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;
import scala.Tuple4;

import java.util.*;
import java.util.stream.Collectors;

public class DistWrapper extends WrapperBase {
    protected DistCpSettings settings;
    private String codec;
    private boolean deleteOnSuccess = false;
    private Map<String, Tuple3<String[], String[], Character>> sinkInfo;

    public DistWrapper(JavaSparkContext context, WrapperConfig config) {
        super(context, config);

        settings = DistCpSettings.fromConfig(wrapperConfig);
    }

    // from, to, group, ?sink
    private void distCpCmd(List<Tuple4<String, String, String, String>> list) {
        CopyFilesFunction cff = new CopyFilesFunction(deleteOnSuccess, codec, sinkInfo);
        context.parallelize(new ArrayList<>(list), list.size())
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

                        List<Tuple3<String, String, String>> splits = DistCpSettings.srcDestGroup(path);
                        for (int i = 0; i < splits.size(); i++) {
                            Tuple3<String, String, String> split = splits.get(i);
                            inputs.add(new Tuple4<>(split._2(), settings.inputDir + "/" + sink + "/part-" + String.format("%05d", i), split._3(), sink));
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
