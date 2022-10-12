/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.transform;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class H3ColumnarToPolygon implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("h3ColumnarToPolygon", StreamType.Columnar, StreamType.Polygon,
                "Take a Columnar DataStream with H3 hashes and produce a Polygon DataStream",

                new DefinitionMetaBuilder()
                        .def("hash.column", "H3 hash column")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get("polygon");
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes("value");
            }

            final String hashColumn = params.get("hash.column");
            if (hashColumn == null) {
                throw new InvalidConfigurationException("Parameter hash.column is required to produce Polygons from Columnar H3 DataStream, " +
                        "but it wasn't specified");
            }

            List<String> _outputColumns = valueColumns;
            final GeometryFactory geometryFactory = new GeometryFactory();

            return new DataStream(StreamType.Polygon, ((JavaRDD<Columnar>) ds.get())
                    .mapPartitions(it -> {
                        List<Polygon> ret = new ArrayList<>();

                        H3Core h3 = H3Core.newInstance();

                        while (it.hasNext()) {
                            Columnar s = it.next();

                            Map<String, Object> props = new HashMap<>();
                            for (String col : _outputColumns) {
                                props.put(col, s.asIs(col));
                            }

                            long hash = Long.parseUnsignedLong(s.asString(hashColumn), 16);
                            List<GeoCoord> geo = h3.h3ToGeoBoundary(hash);
                            geo.add(geo.get(0));

                            List<Coordinate> cl = new ArrayList<>();
                            geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                            Polygon polygon = geometryFactory.createPolygon(cl.toArray(new Coordinate[0]));
                            polygon.setUserData(props);

                            ret.add(polygon);
                        }

                        return ret.iterator();
                    }), newColumns);
        };
    }
}
