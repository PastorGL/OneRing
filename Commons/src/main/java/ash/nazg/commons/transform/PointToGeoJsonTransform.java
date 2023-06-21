/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import ash.nazg.metadata.TransformedStreamMetaBuilder;
import ash.nazg.data.spatial.PointEx;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.wololo.geojson.Feature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class PointToGeoJsonTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("pointToGeoJson", StreamType.Point, StreamType.PlainText,
                "Take a Point DataStream and produce a Plain Text DataStream with GeoJSON fragments",

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

            return new DataStream(StreamType.PlainText, ((JavaRDD<PointEx>) ds.get())
                    .mapPartitions(it -> {
                        List<Text> result = new ArrayList<>();

                        while (it.hasNext()) {
                            PointEx point = it.next();
                            Map<String, Object> props = (Map) point.getUserData();

                            Map<String, Object> featureProps = new HashMap<>();
                            for (String col : _outputColumns) {
                                switch (col) {
                                    case "_center_lat": {
                                        featureProps.put(col, point.getY());
                                        break;
                                    }
                                    case "_center_lon": {
                                        featureProps.put(col, point.getX());
                                        break;
                                    }
                                    case "_radius": {
                                        featureProps.put(col, point.getRadius());
                                        break;
                                    }
                                    default: {
                                        featureProps.put(col, props.get(col));
                                    }
                                }
                            }

                            result.add(new Text(new Feature(new org.wololo.geojson.Point(new double[]{point.getX(), point.getY()}), featureProps).toString()));
                        }

                        return result.iterator();
                    }), newColumns);
        };
    }
}
