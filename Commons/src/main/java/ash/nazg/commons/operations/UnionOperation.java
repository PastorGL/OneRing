/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionEnum;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public class UnionOperation extends Operation {
    public static final String OP_UNION_SPEC = "spec";

    private String rawInput;
    private String outputName;

    private UnionSpec unionSpec;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("union", "Take a number of RDDs (in the form of the list and/or prefixed wildcard)" +
                " and union them into one",

                new PositionalStreamsMetaBuilder()
                        .ds("A list of Plain RDDs to union",
                                new StreamType[]{StreamType.Plain}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_UNION_SPEC, "Union specification", UnionSpec.class,
                                UnionSpec.CONCAT.name(), "By default, just concatenate")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("United RDD",
                                new StreamType[]{StreamType.Passthru}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        rawInput = String.join(",", opResolver.positionalInputs());
        outputName = opResolver.positionalOutput(0);
        unionSpec = opResolver.definition(OP_UNION_SPEC);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        List<String> inputs = getMatchingInputs(input.keySet(), rawInput);

        JavaRDD<Object> output = null;
        List<JavaPairRDD<Object, Integer>> paired = new ArrayList<>();
        final int inpNumber = inputs.size();
        for (int i = 0; i < inpNumber; i++) {
            JavaRDDLike inp = input.get(inputs.get(i));
            final Integer ii = i;

            paired.add(inp.mapToPair(v -> new Tuple2<>(v, ii)));
        }

        JavaPairRDD<Object, Integer> union = ctx.<Object, Integer>union(paired.toArray(new JavaPairRDD[0]));
        switch (unionSpec) {
            case XOR: {
                output = union
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
                output = union
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
            case CONCAT: {
                output = union
                        .keys();
                break;
            }
        }

        return Collections.singletonMap(outputName, output);
    }

    public enum UnionSpec implements DefinitionEnum {
        CONCAT("Just concatenate inputs, don't look into records"),
        XOR("Only emit records that occur strictly in one input RDD"),
        AND("Only emit records that occur in all input RDDs");

        private final String descr;

        UnionSpec(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
