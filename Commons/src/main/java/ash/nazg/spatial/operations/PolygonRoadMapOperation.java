/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.*;

@SuppressWarnings("unused")
public class PolygonRoadMapOperation extends Operation {
    public static final String OP_ROAD_NAME_COL = "name.col";
    public static final String OP_ROAD_TYPE_COL = "type.col";
    public static final String OP_ROAD_WIDTH_COL = "width.col";
    public static final String OP_ROAD_TYPES = "road.types";
    public static final String OP_TYPE_MULTIPLIER_PREFIX = "type.multiplier.";

    private String inputName;
    private String nameColumn;
    private String typeColumn;
    private String widthColumn;

    private String outputName;

    private Map<String, Double> multipliers;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("polygonRoadMap", "Generate a Polygon RDD road map from the GeoJSON fragments with LineString roads",

                new PositionalStreamsMetaBuilder()
                        .ds("Plain RDD with a GeoJSON fragment with LineString or MultiLineString" +
                                        " per each line, together describing a road map of a region",
                                new StreamType[]{StreamType.Plain}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_ROAD_NAME_COL, "Feature attribute with road name")
                        .def(OP_ROAD_TYPE_COL, "Feature attribute with target road type")
                        .def(OP_ROAD_WIDTH_COL, "Feature attribute with road width")
                        .def(OP_ROAD_TYPES, "Target road types", String[].class,
                                "primary,secondary,tertiary", "Default target road types")
                        .dynDef(OP_TYPE_MULTIPLIER_PREFIX, "Multipliers to adjust road width for each target type",
                                Double.class)
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Polygon RDD with each record corresponding to a fragment of a road",
                                new StreamType[]{StreamType.Polygon}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);

        outputName = opResolver.positionalOutput(0);

        String[] roadTypes = opResolver.definition(OP_ROAD_TYPES);
        multipliers = new HashMap<>();
        for (String roadType : roadTypes) {
            Double multiplier = opResolver.definition(OP_TYPE_MULTIPLIER_PREFIX + roadType);
            multipliers.put(roadType, multiplier);
        }

        typeColumn = opResolver.definition(OP_ROAD_TYPE_COL);
        widthColumn = opResolver.definition(OP_ROAD_WIDTH_COL);
        nameColumn = opResolver.definition(OP_ROAD_NAME_COL);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final String _nameColumn = nameColumn;
        final String _typeColumn = typeColumn;
        final String _widthColumn = widthColumn;
        final String[] _attributes = {nameColumn, typeColumn, widthColumn};
        final Map<String, Double> _multipliers = multipliers;

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<Polygon> polygons = ((JavaRDD<Object>) input.get(inputName))
                .flatMap(line -> {
                    List<Polygon> result = new ArrayList<>();

                    GeoJSONReader reader = new GeoJSONReader();

                    GeoJSON json = GeoJSONFactory.create(String.valueOf(line));
                    List<Feature> features = null;
                    if (json instanceof Feature) {
                        features = Collections.singletonList((Feature) json);
                    } else if (json instanceof FeatureCollection) {
                        features = Arrays.asList(((FeatureCollection) json).getFeatures());
                    }

                    if (features != null) {
                        for (Feature feature : features) {
                            Geometry geometry = reader.read(feature.getGeometry());

                            Map<String, Object> featureProps = feature.getProperties();
                            String roadType = String.valueOf(featureProps.get(_typeColumn));
                            Optional<Object> roadName = Optional.ofNullable(featureProps.get(_nameColumn));

                            if (_multipliers.containsKey(roadType) && roadName.isPresent()) {
                                Object width = featureProps.get(_widthColumn);
                                if (width != null) {
                                    List<Geometry> geometries = new ArrayList<>();

                                    if (geometry instanceof LineString) {
                                        geometries.add(geometry);
                                    } else if (geometry instanceof MultiLineString) {
                                        for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                            geometries.add(geometry.getGeometryN(i));
                                        }
                                    }

                                    int numSeg = geometries.size();
                                    if (numSeg > 0) {
                                        MapWritable properties = new MapWritable();
                                        featureProps.forEach((key, value) -> properties.put(new Text(key), new Text(String.valueOf(value))));

                                        for (Geometry g : geometries) {
                                            int pointNum = g.getNumPoints();
                                            if (pointNum > 1) {
                                                LineString ls = (LineString) g;

                                                Point[] trk = new Point[pointNum];
                                                for (int i = 0; i < pointNum; i++) {
                                                    trk[i] = ls.getPointN(i);
                                                }

                                                double radius = Double.parseDouble(String.valueOf(width)) * _multipliers.get(roadType) / 2;

                                                GeodesicData gd;
                                                Coordinate[] c;
                                                Point prevPoint, point;
                                                double prevY, prevX, pointY, pointX, azi2;
                                                for (int i = 0; i < trk.length; i++) {
                                                    point = trk[i];
                                                    pointY = point.getY();
                                                    pointX = point.getX();

                                                    c = new Coordinate[13];
                                                    for (int a = -180, j = 0; a < 180; a += 30, j++) {
                                                        gd = Geodesic.WGS84.Direct(pointY, pointX, a, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                        c[j] = new CoordinateXY(gd.lon2, gd.lat2);
                                                    }
                                                    c[12] = c[0];
                                                    Polygon poly = geometryFactory.createPolygon(c);
                                                    poly.setUserData(properties);
                                                    result.add(poly);

                                                    if (i != 0) {
                                                        prevPoint = trk[i - 1];
                                                        prevY = prevPoint.getY();
                                                        prevX = prevPoint.getX();
                                                        gd = Geodesic.WGS84.Inverse(prevY, prevX, pointY, pointX, GeodesicMask.AZIMUTH);
                                                        azi2 = gd.azi2;

                                                        c = new Coordinate[5];
                                                        gd = Geodesic.WGS84.Direct(pointY, pointX, azi2 + 90.D, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                        c[0] = new CoordinateXY(gd.lon2, gd.lat2);
                                                        gd = Geodesic.WGS84.Direct(pointY, pointX, azi2 - 90.D, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                        c[1] = new CoordinateXY(gd.lon2, gd.lat2);
                                                        gd = Geodesic.WGS84.Direct(prevY, prevX, azi2 - 90.D, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                        c[2] = new CoordinateXY(gd.lon2, gd.lat2);
                                                        gd = Geodesic.WGS84.Direct(prevY, prevX, azi2 + 90.D, radius, GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
                                                        c[3] = new CoordinateXY(gd.lon2, gd.lat2);
                                                        c[4] = c[0];

                                                        if (Orientation.isCCW(c)) {
                                                            ArrayUtils.reverse(c);
                                                        }

                                                        poly = geometryFactory.createPolygon(c);
                                                        poly.setUserData(properties);
                                                        result.add(poly);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, polygons);
    }
}
