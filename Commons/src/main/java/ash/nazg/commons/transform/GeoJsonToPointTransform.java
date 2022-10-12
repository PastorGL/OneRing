/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import ash.nazg.data.spatial.PointEx;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.*;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.*;

@SuppressWarnings("unused")
public class GeoJsonToPointTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("geoJsonToPoint", StreamType.PlainText, StreamType.Point,
                "Take Plain Text representation of GeoJSON fragment file and produce a Point DataStream",

                new DefinitionMetaBuilder()
                        .def("radius.default", "If set, generated Points will have this value in the radius attribute",
                                Double.class, Double.NaN, "By default, don't add radius attribute to Points")
                        .def("radius.prop", "If set, generated Points will use this JSON property as radius",
                                Double.class, null, "By default, don't add radius attribute to Points")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            String radiusColumn = params.get("radius.prop");
            final double defaultRadius = params.get("radius.default");

            List<String> _outputColumns = newColumns.get("point");

            return new DataStream(StreamType.Point, ((JavaRDD<Object>) ds.get())
                    .flatMap(line -> {
                        List<PointEx> result = new ArrayList<>();
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
                                Map<String, Object> properties = feature.getProperties();

                                List<PointEx> points = new ArrayList<>();
                                if (geometry instanceof Polygon) {
                                    points.add(new PointEx(geometry.getCentroid()));
                                } else if (geometry instanceof MultiPolygon) {
                                    for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                        points.add(new PointEx(geometry.getGeometryN(i).getCentroid()));
                                    }
                                } else if (geometry instanceof Point) {
                                    points.add(new PointEx(geometry));
                                } else if (geometry instanceof MultiPoint) {
                                    for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                        points.add(new PointEx(geometry.getGeometryN(i)));
                                    }
                                }

                                for (PointEx point : points) {
                                    Map<String, Object> props = new HashMap<>();
                                    if (_outputColumns != null) {
                                        for (String col : _outputColumns) {
                                            props.put(col, properties.get(col));
                                        }
                                    } else {
                                        props.putAll(properties);
                                    }
                                    point.setUserData(props);

                                    double radius;
                                    if (radiusColumn != null) {
                                        radius = Double.parseDouble(String.valueOf(properties.get(radiusColumn)));
                                    } else {
                                        radius = defaultRadius;
                                    }
                                    point.setRadius(radius);

                                    result.add(point);
                                }
                            }
                        }

                        return result.iterator();
                    }), newColumns);
        };
    }
}
