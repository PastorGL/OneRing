package ash.nazg.spark;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.WrapperConfig;
import ash.nazg.config.tdl.DirVarVal;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.config.PropertiesConfig.COMMA;
import static ash.nazg.config.TaskConfig.DIRECTIVE_SIGIL;

public abstract class TaskRunnerWrapper extends WrapperBase {
    public static final String ITER = "ITER";

    public TaskRunnerWrapper(JavaSparkContext context, WrapperConfig wrapperConfig) {
        super(context, wrapperConfig);
    }

    protected void processTaskChain(Map<String, JavaRDDLike> rdds) throws Exception {
        Map<String, Operation> opChain = new HashMap<>();
        Map<String, OpInfo> ao = Operations.getAvailableOperations();

        List<String> opNames = wrapperConfig.getOperations();
        for (String name : opNames) {
            if (!name.startsWith(DIRECTIVE_SIGIL)) {
                String verb = wrapperConfig.getVerb(name);

                if (!ao.containsKey(verb)) {
                    throw new InvalidConfigValueException("Operation '" + name + "' has unknown verb '" + verb + "'");
                }

                Operation chainedOp;
                try {
                    OpInfo opInfo = ao.get(verb);

                    chainedOp = (Operation) opInfo.getClass().newInstance();
                    chainedOp.initialize(name, context);
                } catch (Exception e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof InvalidConfigValueException) {
                        throw (InvalidConfigValueException) cause;
                    }

                    throw new InvalidConfigValueException("Cannot instantiate operation '" + verb + "' named '" + name + "' with an exception", e);
                }
                opChain.put(name, chainedOp);
            }
        }

        if (opChain.isEmpty()) {
            throw new InvalidConfigValueException("Operation chain hasn't been configured for the task");
        }

        Properties taskVariables = wrapperConfig.getOverrides();

        Random random = new Random();

        int index = 0;
        do {
            index = advance(rdds, opChain, opNames, taskVariables, random, index, false);
        } while (++index < opNames.size());
    }

    private int advance(Map<String, JavaRDDLike> rdds, Map<String, Operation> opChain, List<String> opNames, Properties taskVariables, Random random, Integer index, boolean skip) throws Exception {
        do {
            String name = opNames.get(index);

            if (name.startsWith(DIRECTIVE_SIGIL)) {
                DirVarVal dvv = wrapperConfig.getDirVarVal(name);
                String variable = dvv.variable;
                String defVal = dvv.value;

                switch (dvv.dir) {
                    case ITER: {
                        String varValue = taskVariables.getProperty(variable, defVal);
                        if ((varValue == null) || varValue.trim().isEmpty()) {
                            skip = true;
                        } else {
                            String[] values = Arrays.stream(varValue.split(COMMA)).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
                            int nextIndex = index + 1;
                            for (String val : values) {
                                taskVariables.setProperty(variable, val);
                                taskVariables.setProperty(ITER, Long.toHexString(random.nextLong()));

                                nextIndex = advance(rdds, opChain, opNames, taskVariables, random, index + 1, skip);
                            }

                            index = nextIndex;
                            taskVariables.setProperty(variable, varValue);
                        }
                        break;
                    }
                    case ELSE: {
                        if (skip) {
                            skip = false;

                            index = advance(rdds, opChain, opNames, taskVariables, random, index + 1, skip);
                        } else {
                            skip = true;
                        }

                        break;
                    }
                    case IF: {
                        String varValue = taskVariables.getProperty(variable, defVal);
                        if ((varValue == null) || varValue.trim().isEmpty()) {
                            skip = true;
                        } else {
                            index = advance(rdds, opChain, opNames, taskVariables, random, index + 1, skip);
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
                            value = ((JavaRDD<Object>) rdd).collect().stream().map(String::valueOf).collect(Collectors.joining(COMMA));
                        }
                        if (rdd instanceof JavaPairRDD) {
                            value = ((JavaPairRDD<Object, Object>) rdd).keys().collect().stream().map(String::valueOf).collect(Collectors.joining(COMMA));
                        }

                        taskVariables.setProperty(variable, value);
                        break;
                    }
                }
            } else {
                if (!skip) {
                    callOperation(rdds, opChain, taskVariables, name);
                }
            }
        } while (++index < opNames.size());

        return index;
    }

    private void callOperation(Map<String, JavaRDDLike> rdds, Map<String, Operation> opChain, Properties currentVariables, String name) throws Exception {
        for (String in : rdds.keySet()) {
            JavaRDDLike rdd = rdds.get(in);
            int inputParts = wrapperConfig.inputParts(in);
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

        Operation op = opChain.get(name);
        op.configure(wrapperConfig.getProperties(), currentVariables);
        Map<String, JavaRDDLike> result = op.getResult(Collections.unmodifiableMap(rdds));

        for (String out : result.keySet()) {
            JavaRDDLike rdd = result.get(out);
            int outputParts = wrapperConfig.outputParts(out);
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
}
