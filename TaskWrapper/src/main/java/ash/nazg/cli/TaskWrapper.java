/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.cli;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.WrapperConfig;
import ash.nazg.dist.CpDirection;
import ash.nazg.dist.DistUtils;
import ash.nazg.spark.Operations;
import ash.nazg.spark.WrapperBase;
import ash.nazg.storage.Adapters;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.OutputAdapter;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class TaskWrapper extends WrapperBase {
    protected CpDirection wrapDistCp;
    protected String inputDir;
    protected String outputDir;
    protected String wrapperStorePath;

    protected JavaSparkContext context;
    protected Map<String, JavaRDDLike> result;

    public TaskWrapper(JavaSparkContext context, WrapperConfig wrapperConfig) {
        super(wrapperConfig);

        this.context = context;
        result = new HashMap<>();

        wrapDistCp = CpDirection.parse(wrapperConfig.getDistCpProperty("wrap", "none"));
        inputDir = wrapperConfig.getDistCpProperty("dir.to", "/input");
        outputDir = wrapperConfig.getDistCpProperty("dir.from", "/output");
        wrapperStorePath = wrapperConfig.getDistCpProperty("store", null);
    }

    public void go() throws Exception {
        Operations operations = new Operations(context);
        operations.setTaskConfig(wrapperConfig);

        for (String sink : wrapperConfig.getInputSink()) {
            String path = wrapperConfig.inputPath(sink);

            if (wrapDistCp.to) {
                Map<String, Tuple2<String, String>> splits = DistUtils.globCSVtoRegexMap(path);

                path = splits.entrySet().stream()
                        .map(split -> {
                            if (split.getValue() != null) {
                                return "hdfs://" + inputDir + "/" + split.getKey();
                            }
                            return split.getKey();
                        })
                        .collect(Collectors.joining(","));
            }

            InputAdapter inputAdapter = Adapters.input(path);
            inputAdapter.setContext(context);
            inputAdapter.setProperties(sink, wrapperConfig);
            result.put(sink, inputAdapter.load(path));
        }

        processTaskChain(operations, result);

        Set<String> tees = wrapperConfig.getTeeOutput();

        Set<String> rddNames = result.keySet();
        Set<String> teeNames = new HashSet<>();
        for (String tee : tees) {
            if (tee.endsWith("*")) {
                String t = tee.substring(0, tee.length() - 2);
                for (String name : rddNames) {
                    if (name.startsWith(t)) {
                        teeNames.add(name);
                    }
                }
            } else {
                for (String name : rddNames) {
                    if (name.equals(tee)) {
                        teeNames.add(name);
                    }
                }
            }
        }

        List<String> paths = null;
        if (wrapDistCp.from && (wrapperStorePath != null)) {
            paths = new ArrayList<>();
        }

        for (String teeName : teeNames) {
            JavaRDDLike rdd = result.get(teeName);

            if (rdd != null) {
                String path = wrapperConfig.outputPath(teeName);

                if (Adapters.PATH_PATTERN.matcher(path).matches()) {
                    if (wrapDistCp.from) {
                        path = "hdfs://" + outputDir + "/" + teeName;

                        if (wrapperStorePath != null) {
                            paths.add(path);
                        }
                    }
                } else {
                    throw new InvalidConfigValueException("Output path '" + path + "' of the output '" + teeName + "' must have a protocol specification and point to a subdirectory");
                }

                OutputAdapter outputAdapter = Adapters.output(path);
                outputAdapter.setProperties(teeName, wrapperConfig);
                outputAdapter.save(path, rdd);
            }
        }

        if (wrapDistCp.from && (wrapperStorePath != null)) {
            OutputAdapter outputList = Adapters.output(wrapperStorePath);
            outputList.setProperties("_default", wrapperConfig);
            outputList.save(wrapperStorePath + "/outputs", context.parallelize(paths, 1));
        }
    }
}
