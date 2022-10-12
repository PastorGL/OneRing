/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import ash.nazg.metadata.TransformedStreamMetaBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class PairToColumnarTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("pairToColumnar", StreamType.KeyValue, StreamType.Columnar,
                "Origin Pair DataSet to Columnar",

                null,
                new TransformedStreamMetaBuilder()
                        .genCol("_key", "Key of the Pair")
                        .build()
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> new DataStream(StreamType.Columnar, ((JavaPairRDD<Text, Columnar>) ds.get())
                .mapPartitions(it -> {
                    List<String> valueColumns = newColumns.get("value");
                    if (valueColumns == null) {
                        valueColumns = ds.accessor.attributes("value");
                    }

                    List<String> _outputColumns = valueColumns;
                    int len = _outputColumns.size();

                    List<Columnar> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Tuple2<Text, Columnar> o = it.next();
                        Columnar line = o._2;

                        Columnar rec = new Columnar(valueColumns);
                        for (int i = 0; i < len; i++) {
                            String key = valueColumns.get(i);

                            rec.put(key, key.equalsIgnoreCase("_key") ? String.valueOf(o._1) : String.valueOf(line.asIs(key)));
                        }

                        ret.add(rec);
                    }

                    return ret.iterator();
                }), newColumns);
    }
}
