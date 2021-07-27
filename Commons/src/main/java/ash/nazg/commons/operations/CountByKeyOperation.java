/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class CountByKeyOperation extends Operation {
    private String inputName;

    private String outputName;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("countByKey", "Count values under the same key in a given PairRDD. Output is key to Long count PairRDD.",

                new PositionalStreamsMetaBuilder()
                        .ds("Pair RDD to count values under each unique key",
                                new StreamType[]{StreamType.KeyValue}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .ds("Pair RDD with unique keys and count of values of input RDD under each",
                                new StreamType[]{StreamType.KeyValue}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaPairRDD<Object, Long> out = ((JavaPairRDD<Object, Object>) input.get(inputName))
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Object, Long>> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        ret.add(new Tuple2<>(it.next()._1, 1L));
                    }
                    return ret.iterator();
                })
                .reduceByKey(Long::sum);

        return Collections.singletonMap(outputName, out);
    }
}
