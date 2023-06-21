/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("unused")
public class ColumnarToPairTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToPair", StreamType.Columnar, StreamType.KeyValue,
                "Origin Columnar DataSet to Pair",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get("value");
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes("value");
            }

            List<String> _outputColumns = valueColumns;

            return new DataStream(StreamType.KeyValue, ((JavaPairRDD<String, Columnar>) ds.get())
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<String, Columnar>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<String, Columnar> line = it.next();

                            Columnar rec = new Columnar(_outputColumns);
                            for (String col : _outputColumns) {
                                rec.put(col, line._2.asIs(col));
                            }

                            ret.add(new Tuple2<>(line._1, rec));
                        }

                        return ret.iterator();
                    }), Collections.singletonMap("value", _outputColumns));
        };
    }
}
