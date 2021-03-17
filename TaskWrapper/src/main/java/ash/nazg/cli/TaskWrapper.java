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
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.storage.RDDInfo;

import java.util.*;
import java.util.stream.Collectors;

import static scala.collection.JavaConverters.seqAsJavaList;

public class TaskWrapper extends TaskRunnerWrapper {
    private static final Logger LOG = Logger.getLogger(TaskWrapper.class);

    protected DistCpSettings settings;

    protected Map<String, JavaRDDLike> result;

    public TaskWrapper(JavaSparkContext context, WrapperConfig wrapperConfig) {
        super(context, wrapperConfig);

        result = new HashMap<>();

        settings = DistCpSettings.fromConfig(wrapperConfig);
    }

    public void go() throws Exception {
        final Map<String, Long> recordsRead = new HashMap<>();
        final Map<String, Long> recordsWritten = new HashMap<>();

        context.sc().addSparkListener(new SparkListener() {
            @Override
            public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
                StageInfo stageInfo = stageCompleted.stageInfo();

                long rR = stageInfo.taskMetrics().inputMetrics().recordsRead();
                long rW = stageInfo.taskMetrics().outputMetrics().recordsWritten();
                List<RDDInfo> infos = seqAsJavaList(stageInfo.rddInfos());
                List<String> rddNames = infos.stream()
                        .map(RDDInfo::name)
                        .filter(Objects::nonNull)
                        .filter(n -> n.startsWith("one-ring:"))
                        .collect(Collectors.toList());
                if (rR > 0) {
                    rddNames.forEach(name -> recordsRead.compute(name, (n, r) -> (r == null) ? rR : rR + r));
                }
                if (rW > 0) {
                    rddNames.forEach(name -> recordsWritten.compute(name, (n, w) -> (w == null) ? rW : rW + w));
                }
            }
        });

        List<String> sinks = wrapperConfig.getInputSink();
        for (String sink : sinks) {
            String path = wrapperConfig.inputPath(sink);

            InputAdapter inputAdapter = Adapters.input(path);
            if ((inputAdapter instanceof HadoopAdapter) && settings.toCluster) {
                path = settings.inputDir + "/" + sink + "/part-*";

                inputAdapter = Adapters.input(path);
            }

            inputAdapter.setContext(context);
            inputAdapter.setProperties(sink, wrapperConfig);

            JavaRDDLike inputRdd = inputAdapter.load(path);
            inputRdd.rdd().setName("one-ring:sink:" + sink);
            result.put(sink, inputRdd);
        }

        processTaskChain(result);

        List<String> tees = wrapperConfig.getTeeOutput();

        Set<String> rddNames = result.keySet();
        Set<String> teeNames = new HashSet<>();
        for (String tee : tees) {
            if (tee.endsWith("*")) {
                if (settings.fromCluster && (settings.wrapperStorePath == null)) {
                    throw new InvalidConfigValueException("A call of configuration with wildcard task.tee.output must" +
                            " have wrapper store path set");
                }

                String t = tee.substring(0, tee.length() - 1);
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
        if (settings.fromCluster) {
            paths = new ArrayList<>();
        }

        for (String teeName : teeNames) {
            JavaRDDLike outputRdd = result.get(teeName);

            if (outputRdd != null) {
                outputRdd.rdd().setName("one-ring:tee:" + teeName);
                String path = wrapperConfig.outputPath(teeName);

                OutputAdapter outputAdapter = Adapters.output(path);
                if (settings.fromCluster && (outputAdapter instanceof HadoopAdapter)) {
                    if (Adapters.PATH_PATTERN.matcher(path).matches()) {
                        path = settings.outputDir + "/" + teeName;

                        paths.add(path);
                    } else {
                        throw new InvalidConfigValueException("Output path '" + path + "' of the output '" + teeName + "' must have a protocol specification and point to a subdirectory");
                    }

                    outputAdapter = Adapters.output(path);
                }

                outputAdapter.setProperties(teeName, wrapperConfig);
                outputAdapter.save(path, outputRdd);
            }
        }

        if (settings.fromCluster) {
            OutputAdapter outputList = Adapters.output(settings.wrapperStorePath);
            outputList.setProperties("_default", wrapperConfig);
            outputList.save(settings.wrapperStorePath + "/outputs", context.parallelize(paths, 1));
        }

        List<String> metricsLog = new ArrayList<>();
        metrics.forEach((ds, m) -> {
            metricsLog.add("Metrics of data stream '" + ds + "'");
            m.forEach((k, v) -> metricsLog.add(k + ": " + v));
        });
        metricsLog.forEach(LOG::info);

        String metricsStorePath = wrapperConfig.metricsStorePath();
        if (metricsStorePath != null) {
            OutputAdapter outputList = Adapters.output(metricsStorePath);
            outputList.setProperties("_default", wrapperConfig);
            outputList.save(metricsStorePath, context.parallelize(metricsLog, 1));
        }

        recordsRead.forEach((key, value) -> LOG.info("One Ring sink '" + key + "': " + value + " record(s) read"));
        recordsWritten.forEach((key, value) -> LOG.info("One Ring tee '" + key + "': " + value + " records(s) written"));
    }
}
