package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public class UnionOperation extends Operation {
    @Description("Union specification")
    public static final String OP_UNION_SPEC = "spec";
    @Description("By default, just concatenate")
    public static final UnionSpec DEF_UNION_SPEC = UnionSpec.CONCAT;

    public static final String VERB = "union";

    private String rawInput;
    private String outputName;

    private UnionSpec unionSpec;

    @Override
    @Description("Take a number of RDDs (in the form of the list and/or prefixed wildcard)" +
            " and union them into one")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_UNION_SPEC, UnionSpec.class, DEF_UNION_SPEC),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Passthru},
                                false
                        )
                )
        );
    }

    @Override
    public void setConfig(OperationConfig config) throws InvalidConfigValueException {
        super.setConfig(config);

        rawInput = String.join(",", config.inputs);
        outputName = config.outputs.get(0);
        unionSpec = config.defs.getTyped(OP_UNION_SPEC);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        List<JavaRDDLike> inputs = getMatchingInputs(input, rawInput);

        JavaRDD<Object> output = null;

        if (unionSpec != UnionSpec.CONCAT) {
            List<JavaPairRDD<Object, Integer>> paired = new ArrayList<>();
            final int inpNumber = inputs.size();
            for (int i = 0; i < inpNumber; i++) {
                JavaRDDLike inp = inputs.get(i);
                final Integer ii = i;

                paired.add(inp.mapToPair(v -> new Tuple2<>(v, ii)));
            }

            switch (unionSpec) {
                case XOR: {
                    output = ((JavaPairRDD<Object, Integer>) ctx.<Object, Object>union(paired.toArray(new JavaPairRDD[0])))
                            .groupByKey()
                            .mapValues(it -> {
                                final Set<Integer> inpSet = new HashSet<>();
                                final int[] count = new int[1];
                                it.forEach(i -> {
                                    inpSet.add(i);
                                    count[0]++;
                                });

                                if (inpSet.size() > 1) {
                                    return 0;
                                } else {
                                    return count[0];
                                }
                            })
                            .flatMap(t -> Stream.generate(() -> t._1).limit(t._2).iterator());
                    break;
                }
                case AND: {
                    output = ((JavaPairRDD<Object, Integer>) ctx.<Object, Object>union(paired.toArray(new JavaPairRDD[0])))
                            .groupByKey()
                            .mapValues(it -> {
                                Iterator<Integer> iter = it.iterator();
                                Set<Integer> inpSet = new HashSet<>();
                                int count = 0;
                                while (iter.hasNext()) {
                                    inpSet.add(iter.next());
                                    count++;
                                }
                                if (inpSet.size() < inpNumber) {
                                    return 0;
                                } else {
                                    return count;
                                }
                            })
                            .flatMap(t -> Stream.generate(() -> t._1).limit(t._2).iterator());
                    break;
                }
            }
        } else {
            output = ctx.union(inputs.toArray(new JavaRDD[0]));
        }

        return Collections.singletonMap(outputName, output);
    }

    public enum UnionSpec {
        @Description("Just concatenate inputs, don't look into records")
        CONCAT,
        @Description("Only emit records that occur strictly in one input RDD")
        XOR,
        @Description("Only emit records that occur in all input RDDs")
        AND
    }
}
