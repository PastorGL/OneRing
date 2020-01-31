/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import ash.nazg.cli.TaskWrapper;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.dist.config.DistWrapperConfig;
import ash.nazg.spark.SparkTask;
import ash.nazg.storage.Adapters;
import ash.nazg.cli.config.CpDirection;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class DistWrapper extends TaskWrapper {
    private String exeDistCp;
    private boolean deleteOnSuccess;

    public DistWrapper(DistWrapperConfig wrapperConfig) {
        super(null, wrapperConfig);
    }

    private String distCpCmd(String src, String dest, String groupBy) {
        StringJoiner sj = new StringJoiner(" ");
        sj.add(exeDistCp);
        sj.add("--src=" + src);
        sj.add("--dest=" + dest);
        sj.add("--groupBy=" + groupBy);
        if (deleteOnSuccess) {
            sj.add("--deleteOnSuccess");
        }

        return sj.toString();
    }

    @Override
    public void go() throws Exception {
        DistWrapperConfig taskWrapperConfig = (DistWrapperConfig) wrapperConfig;

        final List<String> lines = new ArrayList<>();
        wrapDistCp = taskWrapperConfig.getCpDirection();
        CpDirection distDirection = taskWrapperConfig.getDistDirection();
        inputDir = taskWrapperConfig.getCpToDir();
        outputDir = taskWrapperConfig.getCpFromDir();
        wrapperStorePath = taskWrapperConfig.getWrapperStorePath();

        if (distDirection.any && wrapDistCp.any) {
            exeDistCp = taskWrapperConfig.getDistCpExe();

            deleteOnSuccess = taskWrapperConfig.getDistCpMove();

            if (distDirection.to && wrapDistCp.to) {
                SparkTask taskHandler = new SparkTask(null);
                taskHandler.setTaskConfig(wrapperConfig);

                for (String sink : wrapperConfig.getInputSink()) {
                    Map<String, Tuple2<String, String>> splits = DistUtils.globCSVtoRegexMap(wrapperConfig.inputPath(sink));

                    splits.forEach((key, value) -> {
                        if (value != null) {
                            lines.add(distCpCmd("s3://" + value._1, "hdfs://" + inputDir + "/" + key, value._2));
                        }
                    });
                }
            }

            if (distDirection.from && wrapDistCp.from) {
                if (wrapperStorePath != null) {
                    List outputs = Files.readAllLines(Paths.get(wrapperStorePath));

                    for (Object output : outputs) {
                        String path = String.valueOf(output);
                        String name = path.substring(("hdfs://" + outputDir + "/").length());

                        lines.add(distCpCmd(path, wrapperConfig.outputPath(name), ".*/(" + name + ".*?)/part.*"));
                    }
                } else {
                    Set<String> teeOutput = taskWrapperConfig.getTeeOutput();

                    for (String name : teeOutput) {
                        String path = taskWrapperConfig.outputPath(name);

                        if (Adapters.PATH_PATTERN.matcher(path).matches()) {
                            lines.add(distCpCmd("hdfs://" + outputDir + "/" + name, path, ".*/(" + name + ".*?)/part.*"));
                        } else {
                            throw new InvalidConfigValueException("Output path '" + path + "' must point to a subdirectory for an output '" + name + "'");
                        }
                    }
                }
            }

            Files.write(Paths.get(taskWrapperConfig.getDistCpIni()), lines);
        }
    }
}
