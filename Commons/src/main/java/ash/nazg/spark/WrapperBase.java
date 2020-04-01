/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
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

public abstract class WrapperBase {
    public static final String APP_NAME = "One Ring";

    public static final String ITER = "ITER";

    protected JavaSparkContext context;
    protected WrapperConfig wrapperConfig;

    public WrapperBase(JavaSparkContext context, WrapperConfig wrapperConfig) {
        this.context = context;
        this.wrapperConfig = wrapperConfig;
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
        Properties currentVariables = new Properties(taskVariables);

        Random random = new Random();

        int level = 0;
        int[] index = new int[opNames.size()];
        boolean[] skip = new boolean[opNames.size()];
        while (index[level] != opNames.size()) {
            String name = opNames.get(index[level]);

            if (name.startsWith(DIRECTIVE_SIGIL)) {
                DirVarVal dvv = wrapperConfig.getDirVarVal(name);
                String variable = dvv.variable;
                String defVal = dvv.value;

                switch (dvv.dir) {
                    case ITER: {
                        level++;

                        String varValue = taskVariables.getProperty(variable, defVal);
                        if ((varValue == null) || varValue.trim().isEmpty()) {
                            skip[level] = true;
                            break;
                        }

                        int thisIndex = index[level];
                        String[] values = Arrays.stream(varValue.split(COMMA)).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
                        for (String val : values) {
                            currentVariables.setProperty(variable, val);
                            currentVariables.setProperty(ITER, Long.toHexString(random.nextLong()));

                            index[level] = thisIndex;
                            while (true) {
                                String opName = opNames.get(++index[level]);
                                if (opName.startsWith(DIRECTIVE_SIGIL)) {
                                    break;
                                }

                                callOperation(rdds, opChain, currentVariables, opName);
                            }
                        }

                        currentVariables.setProperty(variable, varValue);
                        break;
                    }
                    case ELSE : {
                        if (skip[level]) {
                            skip[level] = false;

                            while (true) {
                                String opName = opNames.get(++index[level]);
                                if (opName.startsWith(DIRECTIVE_SIGIL)) {
                                    break;
                                }

                                callOperation(rdds, opChain, currentVariables, opName);
                            }
                        } else {
                            skip[level] = true;
                        }

                        break;
                    }
                    case IF: {
                        level++;

                        String varValue = taskVariables.getProperty(variable, defVal);
                        if ((varValue == null) || varValue.trim().isEmpty()) {
                            skip[level] = true;
                            break;
                        }

                        while (true) {
                            String opName = opNames.get(++index[level]);
                            if (opName.startsWith(DIRECTIVE_SIGIL)) {
                                break;
                            }

                            callOperation(rdds, opChain, currentVariables, opName);
                        }
                        break;
                    }
                    case END: {
                        index[level - 1] = index[level];

                        if (skip[level]) {
                            skip[level] = false;
                        }

                        level--;
                        break;
                    }
                    case LET: {
                        String value = null;

                        JavaRDDLike rdd = rdds.get(defVal);
                        if (rdd instanceof JavaRDD) {
                            value = ((JavaRDD<Object>)rdd).collect().stream().map(String::valueOf).collect(Collectors.joining(COMMA));
                        }
                        if (rdd instanceof JavaPairRDD) {
                            value = ((JavaPairRDD<Object, Object>)rdd).values().collect().stream().map(String::valueOf).collect(Collectors.joining(COMMA));
                        }

                        currentVariables.setProperty(variable, value);
                        break;
                    }
                }
            } else {
                if (skip[level]) {
                    continue;
                }

                callOperation(rdds, opChain, currentVariables, name);
            }

            index[level]++;
        }
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