/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.cli;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.WrapperConfig;
import ash.nazg.dist.DistCpSettings;
import ash.nazg.spark.TaskRunnerWrapper;
import ash.nazg.storage.Adapters;
import ash.nazg.storage.HadoopAdapter;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.OutputAdapter;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;

import java.util.*;

public class TaskWrapper extends TaskRunnerWrapper {
    protected DistCpSettings settings;

    protected Map<String, JavaRDDLike> result;

    public TaskWrapper(JavaSparkContext context, WrapperConfig wrapperConfig) {
        super(context, wrapperConfig);

        result = new HashMap<>();

        settings = DistCpSettings.fromConfig(wrapperConfig);
    }

    public void go() throws Exception {
        List<String> sinks = wrapperConfig.getInputSink();
        for (String sink : sinks) {
            String path = wrapperConfig.inputPath(sink);

            InputAdapter inputAdapter = Adapters.input(path);
            if ((inputAdapter instanceof HadoopAdapter) && settings.toCluster) {
                List<Tuple3<String, String, String>> splits = DistCpSettings.srcDestGroup(path);

                StringJoiner joiner = new StringJoiner(",");
                for (int i = 0; i < splits.size(); i++) {
                    joiner.add(settings.inputDir + "/" + sink + "/part-" + String.format("%05d", i));
                }
                path = joiner.toString();

                inputAdapter = Adapters.input(path);
            }

            inputAdapter.setContext(context);
            inputAdapter.setProperties(sink, wrapperConfig);

            result.put(sink, inputAdapter.load(path));
        }

        processTaskChain(result);

        List<String> tees = wrapperConfig.getTeeOutput();

        Set<String> rddNames = result.keySet();
        Set<String> teeNames = new HashSet<>();
        for (String tee : tees) {
            if (tee.endsWith("*")) {
                if (settings.wrapperStorePath == null) {
                    throw new InvalidConfigValueException("A call of configuration with wildcard task.tee.output must" +
                            " have wrapper store path set");
                }

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
        if (settings.fromCluster && (settings.wrapperStorePath != null)) {
            paths = new ArrayList<>();
        }

        for (String teeName : teeNames) {
            JavaRDDLike rdd = result.get(teeName);

            if (rdd != null) {
                String path = wrapperConfig.outputPath(teeName);

                OutputAdapter outputAdapter = Adapters.output(path);
                if ((outputAdapter instanceof HadoopAdapter) && settings.fromCluster) {
                    if (Adapters.PATH_PATTERN.matcher(path).matches()) {
                        path = settings.outputDir + "/" + teeName;

                        if (settings.wrapperStorePath != null) {
                            paths.add(path);
                        }
                    } else {
                        throw new InvalidConfigValueException("Output path '" + path + "' of the output '" + teeName + "' must have a protocol specification and point to a subdirectory");
                    }

                    outputAdapter = Adapters.output(path);
                }

                outputAdapter.setProperties(teeName, wrapperConfig);
                outputAdapter.save(path, rdd);
            }
        }

        if (settings.fromCluster && (settings.wrapperStorePath != null)) {
            OutputAdapter outputList = Adapters.output(settings.wrapperStorePath);
            outputList.setProperties("_default", wrapperConfig);
            outputList.save(settings.wrapperStorePath + "/outputs", context.parallelize(paths, 1));
        }
    }
}
