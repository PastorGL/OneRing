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
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class DistWrapper extends TaskWrapper {
    private String exeDistCp;
    private String outputIni;
    private String codec;
    private boolean deleteOnSuccess = false;

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

    private void distCpCmd(List<Tuple3<String, String, String>> list) throws IOException {
        if (exeDistCp != null) {
            final List<String> lines = new ArrayList<>();
            for (Tuple3<String, String, String> line : list) {
               lines.add(distCpExe(line._1(), line._2(), line._3()));
            }
            Files.write(Paths.get(outputIni), lines);
        } else {
            CopyFilesFunction cff = new CopyFilesFunction(context.hadoopConfiguration(), deleteOnSuccess, codec);
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

        if (distDirection.any && wrapDistCp.any) {
            exeDistCp = wrapperConfig.getDistCpProperty("exe", null);
            if (exeDistCp != null) {
                outputIni = wrapperConfig.getDistCpProperty("ini", null);
                if (outputIni == null) {
                    throw new InvalidConfigValueException("Requested .ini for external tool '" + exeDistCp + "', but no .ini file path specified");
                }
            }

            codec = wrapperConfig.getDistCpProperty("codec", "none");

            if (distDirection == CpDirection.ONLY_FROM_HDFS) {
                deleteOnSuccess = Boolean.parseBoolean(wrapperConfig.getDistCpProperty("move", "true"));
            }

            if (distDirection.to && wrapDistCp.to) {
                List<Tuple3<String, String, String>> inputs = new ArrayList<>();
                for (String sink : wrapperConfig.getInputSink()) {
                     inputs.addAll(DistUtils.globCSVtoRegexMap(wrapperConfig.inputPath(sink)).entrySet().stream()
                            .flatMap(e -> {
                                Tuple2<String, String> value = e.getValue();

                                List<Tuple3<String, String, String>> ret = new ArrayList<>();
                                if (value != null) {
                                    ret.add(new Tuple3<>("s3://" + value._1, "hdfs://" + inputDir + "/" + e.getKey(), value._2));
                                }

                                return ret.stream();
                            }).collect(Collectors.toList()));
                }

                distCpCmd(inputs);
            }

            if (distDirection.from && wrapDistCp.from) {
                if (wrapperStorePath != null) {
                    List<Tuple3<String, String, String>> outputs = Files.readAllLines(Paths.get(wrapperStorePath)).stream().map(output -> {
                        String path = String.valueOf(output);
                        String name = path.substring(("hdfs://" + outputDir + "/").length());

                        return new Tuple3<>(path, wrapperConfig.outputPath(name), ".*/(" + name + ".*?)/part.*");
                    }).collect(Collectors.toList());

                    distCpCmd(outputs);
                } else {
                    Set<String> tees = wrapperConfig.getTeeOutput();

                    List<Tuple3<String, String, String>> teeList = new ArrayList<>();
                    for (String name : tees) {
                        String path = wrapperConfig.outputPath(name);

                        if (Adapters.PATH_PATTERN.matcher(path).matches()) {
                            teeList.add(new Tuple3<>("hdfs://" + outputDir + "/" + name, path, ".*/(" + name + ".*?)/part.*"));
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
