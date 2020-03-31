/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.WrapperConfig;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.lang.reflect.Constructor;
import java.util.*;

public abstract class WrapperBase {
    public static final String APP_NAME = "One Ring";

    protected JavaSparkContext context;
    protected WrapperConfig wrapperConfig;
    protected Map<String, Operation> opChain = new LinkedHashMap<>();

    public WrapperBase(JavaSparkContext context, WrapperConfig wrapperConfig) {
        this.context = context;
        this.wrapperConfig = wrapperConfig;
    }

    protected void processTaskChain(Map<String, JavaRDDLike> rdds) throws Exception {
        instantiateOperations();
        for (Operation op : opChain.values()) {
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

            op.configure(wrapperConfig.getProperties(), wrapperConfig.getOverrides());
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

    public void instantiateOperations() throws InvalidConfigValueException {
        if (opChain.isEmpty()) {
            Map<String, OpInfo> ao = Operations.getAvailableOperations();

            for (Map.Entry<String, String> operation : wrapperConfig.getOperations().entrySet()) {
                String verb = operation.getValue();
                String name = operation.getKey();

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
            throw new InvalidConfigValueException("Operation chain not configured for the task");
        }

    }
}
