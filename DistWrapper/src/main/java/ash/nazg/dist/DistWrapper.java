/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import ash.nazg.cli.TaskWrapper;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.Adapters;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;
import scala.Tuple4;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class DistWrapper extends TaskWrapper {
    private String exeDistCp;
    private String outputIni;
    private String codec;
    private boolean deleteOnSuccess = false;
    private Map<String, Tuple3<String[], String[], Character>> sinkInfo;

    public DistWrapper(JavaSparkContext context, WrapperConfig config) {
        super(context, config);
    }

    private String distCpExe(String src, String dest, String groupBy) {
        StringJoiner sj = new StringJoiner(" ");
        sj.add(exeDistCp);
        sj.add("--src=" + src);
        sj.add("--dest=" + dest);
        sj.add("--groupBy=" + groupBy);
        sj.add("--outputCodec=" + codec);
        if (deleteOnSuccess) {
            sj.add("--deleteOnSuccess");
        }

        return sj.toString();
    }

    // from, to, group, ?sink
    private void distCpCmd(List<Tuple4<String, String, String, String>> list) throws IOException {
        if (exeDistCp != null) {
            final List<String> lines = new ArrayList<>();
            for (Tuple4<String, String, String, String> line : list) {
               lines.add(distCpExe(line._1(), line._2(), line._3()));
            }
            Files.write(Paths.get(outputIni), lines);
        } else {
            CopyFilesFunction cff = new CopyFilesFunction(deleteOnSuccess, codec, sinkInfo);
            context.parallelize(new ArrayList<>(list), list.size())
                    .foreach(cff);
        }
    }

    @Override
    public void go() throws Exception {
        CpDirection distDirection = CpDirection.parse(wrapperConfig.getDistCpProperty("wrap", "nop"));
        if (distDirection == CpDirection.BOTH_DIRECTIONS) {
            throw new InvalidConfigValueException("DistWrapper's copy direction can't be 'both' because it's ambiguous");
        }

        if (distDirection.anyDirection && wrapDistCp.anyDirection) {
            exeDistCp = wrapperConfig.getDistCpProperty("exe", null);
            if (exeDistCp != null) {
                outputIni = wrapperConfig.getDistCpProperty("ini", null);
                if (outputIni == null) {
                    throw new InvalidConfigValueException("Requested .ini for external tool '" + exeDistCp + "', but no .ini file path specified");
                }
            }

            codec = wrapperConfig.getDistCpProperty("codec", "none");

            if (distDirection == CpDirection.FROM_CLUSTER) {
                deleteOnSuccess = Boolean.parseBoolean(wrapperConfig.getDistCpProperty("move", "true"));
            }

            if (distDirection.toCluster && wrapDistCp.toCluster) {
                List<Tuple4<String, String, String, String>> inputs = new ArrayList<>();

                List<String> sinks = wrapperConfig.getInputSink();
                sinkInfo = new HashMap<>();
                for (String sink : sinks) {
                    String path = wrapperConfig.inputPath(sink);

                    sinkInfo.put(sink, new Tuple3<>(wrapperConfig.getSinkSchema(sink), wrapperConfig.getSinkColumns(sink), wrapperConfig.getSinkDelimiter(sink)));

                    List<Tuple3<String, String, String>> splits = DistUtils.globCSVtoRegexMap(path);
                    for (int i = 0; i < splits.size(); i++) {
                        Tuple3<String, String, String> split = splits.get(i);
                        inputs.add(new Tuple4<>(split._2(), inputDir + "/" + sink + "/part-" + String.format("%05d", i), split._3(), sink));
                    }
                }

                distCpCmd(inputs);
            }

            if (distDirection.fromCluster && wrapDistCp.fromCluster) {
                if (wrapperStorePath != null) {
                    List<Tuple4<String, String, String, String>> outputs = Files.readAllLines(Paths.get(wrapperStorePath)).stream().map(output -> {
                        String path = String.valueOf(output);
                        String name = path.substring((outputDir + "/").length());

                        return new Tuple4<>(path, wrapperConfig.outputPath(name), ".*/(" + name + ".*?)/part.*", (String)null);
                    }).collect(Collectors.toList());

                    distCpCmd(outputs);
                } else {
                    List<String> tees = wrapperConfig.getTeeOutput();

                    List<Tuple4<String, String, String, String>> teeList = new ArrayList<>();
                    for (String name : tees) {
                        String path = wrapperConfig.outputPath(name);

                        if (Adapters.PATH_PATTERN.matcher(path).matches()) {
                            teeList.add(new Tuple4<>(outputDir + "/" + name, path, ".*/(" + name + ".*?)/part.*", null));
                        } else {
                            throw new InvalidConfigValueException("Output path '" + path + "' must point to a subdirectory for an output '" + name + "'");
                        }
                    }

                    distCpCmd(teeList);
                }
            }
        }
    }
}
