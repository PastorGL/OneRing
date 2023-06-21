/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import ash.nazg.data.spatial.PolygonEx;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.*;

@SuppressWarnings("unused")
public class GeoJsonToPolygonTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("geoJsonToPolygon", StreamType.PlainText, StreamType.Polygon,
                "Take Plain Text representation of GeoJSON fragment file and produce a Polygon DataStream",

                null,
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> _outputColumns = newColumns.get("polygon");

            return new DataStream(StreamType.Polygon, ((JavaRDD<Object>) ds.get())
                    .flatMap(line -> {
                        List<PolygonEx> result = new ArrayList<>();
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

                                List<Geometry> geometries = new ArrayList<>();
                                if (geometry instanceof Polygon) {
                                    geometries.add(geometry);
                                } else if (geometry instanceof MultiPolygon) {
                                    for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                        geometries.add(geometry.getGeometryN(i));
                                    }
                                }

                                for (Geometry polygon : geometries) {
                                    Map<String, Object> props = new HashMap<>();
                                    if (_outputColumns != null) {
                                        for (String col : _outputColumns) {
                                            props.put(col, properties.get(col));
                                        }
                                    } else {
                                        props.putAll(properties);
                                    }
                                    polygon.setUserData(props);

                                    result.add(new PolygonEx(polygon));
                                }
                            }
                        }

                        return result.iterator();
                    }), newColumns);
        };
    }
}
