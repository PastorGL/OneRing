/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.functions;

import ash.nazg.spatial.config.ConfigurationParameters;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class PolygonGeoJSONMapper implements FlatMapFunction<Object, Polygon> {
    private final List<String> outputColumns;
    private Text latAttr;
    private Text lonAttr;

    public PolygonGeoJSONMapper(List<String> outputColumns) {
        this.outputColumns = outputColumns.stream()
                .map(c -> c.replaceFirst("^[^.]+\\.", ""))
                .collect(Collectors.toList());
    }

    @Override
    public Iterator<Polygon> call(Object line) {
        List<Polygon> result = new ArrayList<>();
        GeoJSONReader reader = new GeoJSONReader();

        GeoJSON json = GeoJSONFactory.create(String.valueOf(line));

        this.latAttr = new Text(ConfigurationParameters.GEN_CENTER_LAT);
        this.lonAttr = new Text(ConfigurationParameters.GEN_CENTER_LON);

        if (json instanceof Feature) {
            Feature feature = (Feature) json;

            feature(result, reader, feature);
        } else if (json instanceof FeatureCollection) {
            FeatureCollection collection = (FeatureCollection) json;

            for (Feature feature : collection.getFeatures()) {
                feature(result, reader, feature);
            }
        }

        return result.iterator();
    }

    private void feature(List<Polygon> result, GeoJSONReader reader, Feature feature) {
        Geometry geometry = reader.read(feature.getGeometry());

        MapWritable properties = new MapWritable();

        feature.getProperties().entrySet().stream()
                .filter(e -> (outputColumns.size() == 0) || outputColumns.contains(e.getKey()))
                .forEach(e -> properties.put(new Text(e.getKey()), new Text(String.valueOf(e.getValue()))));

        if (geometry instanceof Polygon) {
            Polygon polygon = (Polygon) geometry;

            polygon(result, properties, polygon);
        } else if (geometry instanceof MultiPolygon) {
            for (int i = 0; i < geometry.getNumGeometries(); i++) {
                Geometry g = geometry.getGeometryN(i);
                if (g instanceof Polygon) {
                    Polygon polygon = (Polygon) g;
                    polygon(result, properties, polygon);
                }
            }
        }
    }

    private void polygon(List<Polygon> result, MapWritable properties, Polygon polygon) {
        MapWritable props = new MapWritable();

        props.putAll(properties);
        Point centroid = polygon.getCentroid();
        props.put(latAttr, new DoubleWritable(centroid.getY()));
        props.put(lonAttr, new DoubleWritable(centroid.getX()));

        polygon.setUserData(props);
        result.add(polygon);
    }
}