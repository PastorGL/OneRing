/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.spark.Operation;
import ash.nazg.spark.Operations;
import ash.nazg.spark.RDDMetricsPseudoOperation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.util.stream.Collectors;

public class Interpreter {
    public final Map<String, Map<String, Double>> metrics = new HashMap<>();

    private final TaskDefinitionLanguage.Task taskConfig;
    private final StreamResolver dsResolver;
    private final JavaSparkContext context;

    public Interpreter(TaskDefinitionLanguage.Task taskConfig, JavaSparkContext context) {
        this.taskConfig = taskConfig;
        this.context = context;

        dsResolver = new StreamResolver(taskConfig.dataStreams);
    }

    public void processTaskChain(Map<String, JavaRDDLike> rdds) throws Exception {
        TaskDefinitionLanguage.Definitions metricsLayer = taskConfig.foreignLayer(Constants.METRICS_LAYER);
        if (!metricsLayer.isEmpty()) {
            taskConfig.taskItems.add(0, new TaskDefinitionLanguage.Directive() {{
                directive = "$METRICS{:input}";
            }});
            taskConfig.taskItems.add(new TaskDefinitionLanguage.Directive() {{
                directive = "$METRICS{:output}";
            }});
        }

        int index = 0;
        do {
            index = advance(rdds, new Random(), index, false);
        } while (++index < taskConfig.taskItems.size());
    }

    private int advance(Map<String, JavaRDDLike> rdds, Random random, Integer index, boolean skip) throws Exception {
        do {
            TaskDefinitionLanguage.TaskItem ti = taskConfig.taskItems.get(index);

            if (ti instanceof TaskDefinitionLanguage.Directive) {
                DirVarVal dvv = ((TaskDefinitionLanguage.Directive) ti).dirVarVal();
                String variable = dvv.variable;
                String defVal = dvv.value;

                switch (dvv.dir) {
                    case ITER: {
                        String varValue = taskConfig.variables.getOrDefault(variable, defVal);
                        if ((varValue == null) || varValue.trim().isEmpty()) {
                            skip = true;
                        } else {
                            String[] values = Arrays.stream(varValue.split(Constants.COMMA)).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
                            int nextIndex = index + 1;
                            for (String val : values) {
                                taskConfig.variables.put(variable, val);
                                taskConfig.variables.put(Constants.VAR_ITER, Long.toHexString(random.nextLong()));

                                nextIndex = advance(rdds, random, index + 1, skip);
                            }

                            index = nextIndex;
                            taskConfig.variables.put(variable, varValue);
                        }
                        break;
                    }
                    case ELSE: {
                        if (skip) {
                            skip = false;

                            index = advance(rdds, random, index + 1, skip);
                        } else {
                            skip = true;
                        }

                        break;
                    }
                    case IF: {
                        String varValue = taskConfig.variables.getOrDefault(variable, defVal);
                        if ((varValue == null) || varValue.trim().isEmpty()) {
                            skip = true;
                        } else {
                            index = advance(rdds, random, index + 1, skip);
                        }
                        break;
                    }
                    case END: {
                        return index;
                    }
                    case LET: {
                        String value = null;

                        JavaRDDLike rdd = rdds.get(defVal);
                        if (rdd instanceof JavaRDD) {
                            value = ((JavaRDD<Object>) rdd).collect().stream().map(String::valueOf).collect(Collectors.joining(Constants.COMMA));
                        }
                        if (rdd instanceof JavaPairRDD) {
                            value = ((JavaPairRDD<Object, Object>) rdd).keys().collect().stream().map(String::valueOf).collect(Collectors.joining(Constants.COMMA));
                        }

                        taskConfig.variables.put(variable, value);
                        break;
                    }
                    case METRICS: {
                        String scope = (variable.isEmpty()) ? defVal : variable;

                        callMetrics(scope, rdds);
                        break;
                    }
                }
            } else {
                if (!skip) {
                    callOperation(rdds, (TaskDefinitionLanguage.Operation) ti);
                }
            }
        } while (++index < taskConfig.taskItems.size());

        return index;
    }

    private void callOperation(Map<String, JavaRDDLike> rdds, TaskDefinitionLanguage.Operation op) throws Exception {
        for (String in : rdds.keySet()) {
            JavaRDDLike rdd = rdds.get(in);
            int inputParts = dsResolver.inputParts(in);
            if (inputParts > 0) {
                if (rdd.getNumPartitions() != inputParts) {
                    if (rdd instanceof JavaRDD) {
                        rdd = ((JavaRDD) rdd).repartition(inputParts);
                    }
                    if (rdd instanceof JavaPairRDD) {
                        rdd = ((JavaPairRDD) rdd).repartition(inputParts);
                    }
                }

                rdds.replace(in, rdd);
            }
        }

        if (!Operations.availableOperations.containsKey(op.verb)) {
            throw new InvalidConfigValueException("Operation '" + op.name + "' has unknown verb '" + op.verb + "'");
        }

        Operation chainedOp = Operations.availableOperations.get(op.verb).opClass.newInstance();
        chainedOp.initialize(context);
        chainedOp.configure(op, taskConfig.dataStreams);
        Map<String, JavaRDDLike> result = chainedOp.getResult(Collections.unmodifiableMap(rdds));

        for (String out : result.keySet()) {
            JavaRDDLike rdd = result.get(out);
            int outputParts = dsResolver.outputParts(out);
            if (outputParts > 0) {
                if (rdd.getNumPartitions() != outputParts) {
                    if (rdd instanceof JavaRDD) {
                        rdd = ((JavaRDD) rdd).repartition(outputParts);
                    }
                    if (rdd instanceof JavaPairRDD) {
                        rdd = ((JavaPairRDD) rdd).repartition(outputParts);
                    }
                }
            }
            rdds.putIfAbsent(out, rdd);
        }
    }

    private void callMetrics(String target, Map<String, JavaRDDLike> rdds) throws Exception {
        if ("input".equals(target)) {
            target = String.join(Constants.COMMA, taskConfig.input);
        }
        if ("output".equals(target)) {
            target = String.join(Constants.COMMA, taskConfig.output);
        }

        TaskDefinitionLanguage.Operation metricsProps = TaskDefinitionLanguage.createOperation(taskConfig);
        metricsProps.name = "metrics:" + target;
        metricsProps.inputs.positionalNames = target;
        metricsProps.definitions = taskConfig.foreignLayer(Constants.METRICS_LAYER);

        RDDMetricsPseudoOperation metricsOp = Operations.getMetricsOperation();
        metricsOp.initialize(context);
        metricsOp.configure(metricsProps, taskConfig.dataStreams);
        metricsOp.getResult(rdds);

        metrics.putAll(metricsOp.getMetrics());
    }
}
