/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.cli;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.*;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.storage.RDDInfo;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static scala.collection.JavaConverters.seqAsJavaList;

public class TaskRunner {
    private static final Logger LOG = Logger.getLogger(TaskRunner.class);

    private final JavaSparkContext context;
    private final TaskDefinitionLanguage.Task taskConfig;

    public TaskRunner(JavaSparkContext context, TaskDefinitionLanguage.Task config) {
        this.context = context;
        this.taskConfig = config;
    }

    public void go(boolean local) throws Exception {
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

        StreamResolver dsResolver = new StreamResolver(taskConfig.dataStreams);

        Map<String, JavaRDDLike> result = new HashMap<>();

        for (String input : taskConfig.input) {
            String path = dsResolver.inputPath(input);

            JavaRDDLike inputRdd = context.textFile(path, Math.max(dsResolver.inputParts(input), 1));
            inputRdd.rdd().setName("one-ring:input:" + input);
            result.put(input, inputRdd);
        }

        String wrapperStorePath = taskConfig.getForeignValue("dist.store");
        if (!local && (wrapperStorePath == null)) {
            throw new InvalidConfigValueException("An invocation on the cluster must have wrapper store path set");
        }

        Interpreter interpreter = new Interpreter(taskConfig, context);
        interpreter.processTaskChain(result);

        Set<String> rddNames = result.keySet();
        Set<String> outputNames = new HashSet<>();
        for (String output : taskConfig.output) {
            if (output.endsWith("*")) {
                String t = output.substring(0, output.length() - 1);
                for (String name : rddNames) {
                    if (name.startsWith(t)) {
                        outputNames.add(name);
                    }
                }
            } else {
                for (String name : rddNames) {
                    if (name.equals(output)) {
                        outputNames.add(name);
                    }
                }
            }
        }

        boolean wrapperStore = wrapperStorePath != null;
        List<Tuple2<String, String>> paths = null;
        if (wrapperStore) {
            paths = new ArrayList<>();
        }

        for (String output : outputNames) {
            JavaRDDLike outputRdd = result.get(output);

            if (outputRdd != null) {
                outputRdd.rdd().setName("one-ring:output:" + output);
                String path = dsResolver.outputPath(output);

                if (wrapperStore) {
                    path = dsResolver.outputPath(Constants.DEFAULT_DS) + "/" + output;

                    paths.add(new Tuple2<>(output, path));
                }

                save(path, dsResolver.outputDelimiter(output), outputRdd);
            }
        }

        if (wrapperStore) {
            save(wrapperStorePath + "/outputs", dsResolver.outputDelimiter(Constants.OUTPUTS_DS), context.parallelizePairs(paths, 1));
        }

        if (interpreter.metered()) {
            JavaRDDLike metricsRdd = result.get(Constants.METRICS_DS);
            char metricsDelimiter = dsResolver.outputDelimiter(Constants.METRICS_DS);

            LayerResolver metricsResolver = new LayerResolver(taskConfig.foreignLayer(Constants.METRICS_LAYER));
            String metricsStorePath = metricsResolver.get("store");
            if (metricsStorePath != null) {
                save(metricsStorePath, metricsDelimiter, metricsRdd);
            }

            List<Text> metrics = metricsRdd.collect();
            List<String> metricsLog = new ArrayList<>();
            CSVParser parser = new CSVParserBuilder().withSeparator(metricsDelimiter).build();
            for (Text line : metrics) {
                String[] metric = parser.parseLine(String.valueOf(line));

                metricsLog.add("Metrics of data stream '" + metric[0] + "'");
                metricsLog.add("Total " + metric[1] + ": " + metric[2]);
                metricsLog.add("Counted by '" + metric[3] + "': " + metric[4]);
                metricsLog.add("Average per counter: " + metric[5]);
                metricsLog.add("Median per counter: " + metric[6]);
            }
            metricsLog.forEach(LOG::info);
        }

        LOG.info("One Ring raw physical record statistics");
        recordsRead.forEach((key, value) -> LOG.info("Input '" + key + "': " + value + " record(s) read"));
        recordsWritten.forEach((key, value) -> LOG.info("Output '" + key + "': " + value + " records(s) written"));
    }

    private void save(String path, char delimiter, JavaRDDLike rdd) {
        if (rdd instanceof JavaPairRDD) {
            final String _delimiter = String.valueOf(delimiter);
            ((JavaPairRDD<Object, Object>) rdd).mapPartitions(it -> {
                List<Text> ret = new ArrayList<>();

                while (it.hasNext()) {
                    Tuple2 v = it.next();

                    ret.add(new Text(v._1 + _delimiter + v._2));
                }

                return ret.iterator();
            }).saveAsTextFile(path);
        }

        if (rdd instanceof JavaRDD) {
            rdd.saveAsTextFile(path);
        }
    }
}
