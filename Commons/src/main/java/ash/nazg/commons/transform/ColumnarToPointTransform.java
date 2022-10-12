/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import ash.nazg.data.spatial.PointEx;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.*;

@SuppressWarnings("unused")
public class ColumnarToPointTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToPoint", StreamType.Columnar, StreamType.Point,
                "Take a Columnar DataStream and produce a Point DataStream",

                new DefinitionMetaBuilder()
                        .def("radius.default", "If set, generated Points will have this value in the radius parameter",
                                Double.class, Double.NaN, "By default, Point radius attribute is not set")
                        .def("radius.column", "If set, generated Points will take their radius parameter from the specified column instead",
                                null, "By default, don't set Point radius attribute")
                        .def("lat.column", "Point latitude column")
                        .def("lon.column", "Point longitude column")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get("point");
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes("value");
            }

            final List<String> _outputColumns = valueColumns;

            final String latColumn = params.get("lat.column");
            final String lonColumn = params.get("lon.column");
            final String radiusColumn = params.get("radius.column");
            final Double defaultRadius = params.get("radius.default");

            if ((latColumn == null) || (lonColumn == null)) {
                throw new InvalidConfigurationException("Parameters lat.column and lon.column are both required to produce Points from Columnar DataStream, " +
                        "but those wasn't specified");
            }

            final GeometryFactory geometryFactory = new GeometryFactory();
            final CoordinateSequenceFactory csFactory = geometryFactory.getCoordinateSequenceFactory();

            return new DataStream(StreamType.Point, ((JavaRDD<Columnar>) ds.get())
                    .mapPartitions(it -> {
                        List<PointEx> result = new ArrayList<>();

                        while (it.hasNext()) {
                            Columnar line = it.next();

                            double lat = line.asDouble(latColumn);
                            double lon = line.asDouble(lonColumn);

                            double radius = (radiusColumn != null) ? line.asDouble(radiusColumn) : defaultRadius;

                            Map<String, Object> props = new HashMap<>();
                            for (String col : _outputColumns) {
                                props.put(col, line.asIs(col));
                            }

                            PointEx point = new PointEx(csFactory.create(new Coordinate[]{new Coordinate(lon, lat, radius)}), geometryFactory);
                            point.setUserData(props);

                            result.add(point);
                        }

                        return result.iterator();
                    }), Collections.singletonMap("point", _outputColumns));
        };
    }
}
