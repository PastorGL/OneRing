/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import ash.nazg.metadata.TransformedStreamMetaBuilder;
import ash.nazg.data.spatial.PointEx;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class PointToColumnarTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("pointToColumnar", StreamType.Point, StreamType.Columnar,
                "Take a Point DataStream and produce a Columnar one",

                null,
                new TransformedStreamMetaBuilder()
                        .genCol("_center_lat", "Point latitude")
                        .genCol("_center_lon", "Point longitude")
                        .genCol("_radius", "Point radius")
                        .build()
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get("value");
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes("point");
            }

            List<String> _outputColumns = valueColumns;

            return new DataStream(StreamType.Columnar, ((JavaRDD<PointEx>) ds.get())
                    .mapPartitions(it -> {
                        List<Columnar> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            PointEx t = it.next();
                            Map<String, Object> props = (Map) t.getUserData();

                            Columnar out = new Columnar(_outputColumns);
                            for (String col : _outputColumns) {
                                switch (col) {
                                    case "_center_lat": {
                                        out.put(col, t.getY());
                                        break;
                                    }
                                    case "_center_lon": {
                                        out.put(col, t.getX());
                                        break;
                                    }
                                    case "_radius": {
                                        out.put(col, t.getRadius());
                                        break;
                                    }
                                    default: {
                                        out.put(col, props.get(col));
                                    }
                                }

                            }

                            ret.add(out);
                        }

                        return ret.iterator();
                    }), newColumns);
        };
    }
}
