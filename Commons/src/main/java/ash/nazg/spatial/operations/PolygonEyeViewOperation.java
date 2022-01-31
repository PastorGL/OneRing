package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicMask;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class PolygonEyeViewOperation extends Operation {
    public static final String OP_POI_AZIMUTH_COL = "pois.azimuth.col";
    public static final String OP_POI_VIEWING_ANGLE_COL = "pois.angle.col";
    public static final String OP_DEFAULT_VIEWING_ANGLE = "angle.default";

    private String inputPoisName;
    private String azimuthColumn;
    private String angleColumn;

    private Double defaultAngle;

    private String outputName;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("polygonEyeView", "Create eye view polygons for POIs with set azimuth and view angle",

                new PositionalStreamsMetaBuilder()
                        .ds("Source POI Point RDD with _radius attribute set",
                                new StreamType[]{StreamType.Point}, false
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_POI_AZIMUTH_COL, "Azimuth attribute of POIs, degrees. Counts clockwise from north, +90 is due east, -90 is due west")
                        .def(OP_POI_VIEWING_ANGLE_COL, "Viewing angle attribute of POIs, degrees", null, "By default, viewing angle column isn't set")
                        .def(OP_DEFAULT_VIEWING_ANGLE, "Default viewing angle of POIs, degrees", Double.class,
                                "110", "By default, viewing angle of POIs is 110 degrees")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Output with eye view polygons",
                                new StreamType[]{StreamType.Polygon}, true
                        )
                        .genCol("_azimuth", "Azimuth attribute")
                        .genCol("_angle", "Viewing angle attribute")
                        .genCol("_radius", "Radius attribute")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputPoisName = opResolver.positionalInput(0);

        azimuthColumn = opResolver.definition(OP_POI_AZIMUTH_COL);
        angleColumn = opResolver.definition(OP_POI_VIEWING_ANGLE_COL);
        defaultAngle = opResolver.definition(OP_DEFAULT_VIEWING_ANGLE);

        outputName = opResolver.positionalOutput(0);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final String _azimuthColumn = azimuthColumn.substring(inputPoisName.length() + 1);
        final String _angleColumn = (angleColumn != null) ? angleColumn.substring(inputPoisName.length() + 1) : null;
        final double _defaultAngle = defaultAngle;

        JavaRDD<Point> poisInput = (JavaRDD<Point>) input.get(inputPoisName);

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<Polygon> output = poisInput
                .mapPartitions(it -> {
                    List<Polygon> ret = new ArrayList<>();

                    Text radiusAttr = new Text("_radius");
                    Text azimuthAttr = new Text("_azimuth");
                    Text angleAttr = new Text("_angle");

                    Text azimuthCol = new Text(_azimuthColumn);
                    Text angleCol = (_angleColumn != null) ? new Text(_angleColumn) : null;

                    double radius, azimuth, angle = _defaultAngle;
                    double lat, lon, angleInc;
                    Coordinate[] coords;
                    GeodesicData gd;
                    while (it.hasNext()) {
                        Point poi = it.next();

                        MapWritable props = (MapWritable) poi.getUserData();

                        radius = ((DoubleWritable) props.get(radiusAttr)).get();
                        azimuth = Double.parseDouble(props.get(azimuthCol).toString());
                        if (angleCol != null) {
                            angle = Double.parseDouble(props.get(angleCol).toString());
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

                        Polygon poly = geometryFactory.createPolygon(coords);
                        props.put(azimuthAttr, new DoubleWritable(azimuth));
                        props.put(angleAttr, new DoubleWritable(angle));
                        poly.setUserData(props);

                        ret.add(poly);
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
