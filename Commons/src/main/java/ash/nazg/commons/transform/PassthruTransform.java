/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

@SuppressWarnings("unused")
public class PassthruTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("passthru", StreamType.Passthru, StreamType.Passthru,
                "Doesn't change DataStream in any way except its count of partitions",

                new DefinitionMetaBuilder()
                        .def("part_count", "If set, change count of partitions to desired value",
                                Integer.class, null, "By default, don't change number of parts")
                        .build(),
                null
        );

    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            JavaRDDLike source = ds.get();

            int partCount = source.getNumPartitions();
            int reqParts = params.containsKey("part_count") ? Math.max(params.get("part_count"), 1) : partCount;

            JavaRDDLike output = source;
            if (reqParts != partCount) {
                if (source instanceof JavaRDD) {
                    output = ((JavaRDD) source).repartition(reqParts);
                } else {
                    output = ((JavaPairRDD) source).repartition(reqParts);
                }
            }

            return new DataStream(ds.streamType, output, ds.accessor.attributes());
        };
    }
}
