package ash.nazg.spark;

import ash.nazg.config.WrapperConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.Collections;
import java.util.Map;

public abstract class WrapperBase {
    public static final String APP_NAME = "One Ring";

    protected WrapperConfig wrapperConfig;

    public WrapperBase(WrapperConfig wrapperConfig) {
        this.wrapperConfig = wrapperConfig;
    }

    protected void processTaskChain(SparkTask sparkTask, Map<String, JavaRDDLike> rdds) throws Exception {
        for (Operation op : sparkTask.instantiateOperations()) {
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
}
