/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.metadata.OperationMeta;
import ash.nazg.metadata.Origin;
import ash.nazg.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.data.DataStream;
import ash.nazg.data.StreamType;
import ash.nazg.scripting.Operation;
import ash.nazg.data.spatial.PointEx;
import ash.nazg.data.spatial.PolygonEx;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicMask;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class PolygonEyeViewOperation extends Operation {
    public static final String OP_POI_AZIMUTH_PROP = "azimuth.prop";
    public static final String OP_POI_VIEWING_ANGLE_PROP = "angle.prop";
    public static final String OP_DEFAULT_VIEWING_ANGLE = "angle.default";
    static final String GEN_AZIMUTH = "_azimuth";
    static final String GEN_ANGLE = "_angle";
    static final String GEN_RADIUS = "_radius";

    private String azimuthColumn;
    private String angleColumn;

    private Double defaultAngle;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("polygonEyeView", "Create eye view polygons for POIs with set azimuth and view angle",

                new PositionalStreamsMetaBuilder()
                        .input("Source Points Of Interest DataStream (with set radius)",
                                new StreamType[]{StreamType.Point}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_POI_AZIMUTH_PROP, "Azimuth attribute of POIs, degrees. Counts clockwise from north, +90 is due east, -90 is due west")
                        .def(OP_POI_VIEWING_ANGLE_PROP, "Viewing angle attribute of POIs, degrees", null, "By default, viewing angle column isn't set")
                        .def(OP_DEFAULT_VIEWING_ANGLE, "Default viewing angle of POIs, degrees", Double.class,
                                110.D, "By default, viewing angle of POIs is 110 degrees")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("Output with eye view polygons",
                                new StreamType[]{StreamType.Polygon}, Origin.GENERATED, null
                        )
                        .generated(GEN_AZIMUTH, "Azimuth property")
                        .generated(GEN_ANGLE, "Viewing angle property")
                        .generated(GEN_RADIUS, "Radius property")
                        .generated("*", "All other source properties are added unchanged")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        azimuthColumn = params.get(OP_POI_AZIMUTH_PROP);
        angleColumn = params.get(OP_POI_VIEWING_ANGLE_PROP);
        defaultAngle = params.get(OP_DEFAULT_VIEWING_ANGLE);
    }

    @Override
    public Map<String, DataStream> execute() {
        final String _azimuthColumn = azimuthColumn;
        final String _angleColumn = (angleColumn != null) ? angleColumn : null;
        final double _defaultAngle = defaultAngle;

        DataStream input = inputStreams.getValue(0);
        JavaRDD<PointEx> poisInput = (JavaRDD<PointEx>) input.get();

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<PolygonEx> output = poisInput
                .mapPartitions(it -> {
                    List<PolygonEx> ret = new ArrayList<>();

                    double radius, azimuth, angle = _defaultAngle;
                    double lat, lon, angleInc;
                    Coordinate[] coords;
                    GeodesicData gd;
                    while (it.hasNext()) {
                        PointEx poi = it.next();

                        radius = poi.getRadius();
                        azimuth = poi.asDouble(_azimuthColumn);
                        if (_angleColumn != null) {
                            angle = poi.asDouble(_angleColumn);
                        }

                        coords = new Coordinate[15];

                        lat = poi.getY();
                        lon = poi.getX();
                        coords[14] = coords[0] = new Coordinate(lon, lat);
                        angleInc = angle / 12;

                        for (int i = -6; i <= 6; i++) {
                            gd = Geodesic.WGS84.Direct(lat, lon, azimuth + angleInc * i, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                            coords[i + 7] = new Coordinate(gd.lon2, gd.lat2);
                        }

                        PolygonEx poly = new PolygonEx(geometryFactory.createPolygon(coords));
                        poly.put((Map) poi.getUserData());
                        poly.put(GEN_AZIMUTH, azimuth);
                        poly.put(GEN_ANGLE, angle);
                        poly.put(GEN_RADIUS, radius);

                        ret.add(poly);
                    }

                    return ret.iterator();
                });

        List<String> outputColumns = new ArrayList<>(input.accessor.attributes("point"));
        outputColumns.add(GEN_ANGLE);
        outputColumns.add(GEN_AZIMUTH);
        outputColumns.add(GEN_RADIUS);
        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(StreamType.Polygon, output, Collections.singletonMap("polygon", outputColumns)));
    }
}
