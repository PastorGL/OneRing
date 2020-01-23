package ash.nazg.spatial.functions;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.*;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

public class PointGeoJSONMapper implements FlatMapFunction<Object, Point> {
    final private GeometryFactory geometryFactory = new GeometryFactory();
    private final List<String> outputColumns;
    private Text latAttr;
    private Text lonAttr;
    private Double defaultRadius;
    private Text radiusAttr;

    public PointGeoJSONMapper(Double defaultRadius, List<String> outputColumns) {
        this.outputColumns = outputColumns.stream()
                .map(c -> c.replaceFirst("^[^.]+\\.", ""))
                .collect(Collectors.toList());

        this.defaultRadius = defaultRadius;
    }

    @Override
    public Iterator<Point> call(Object line) {
        List<Point> result = new ArrayList<>();
        GeoJSONReader reader = new GeoJSONReader();

        GeoJSON json = GeoJSONFactory.create(String.valueOf(line));

        this.latAttr = new Text(GEN_CENTER_LAT);
        this.lonAttr = new Text(GEN_CENTER_LON);
        if (defaultRadius != null) {
            this.radiusAttr = new Text(GEN_RADIUS);
        }

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

    private void feature(List<Point> result, GeoJSONReader reader, Feature feature) {
        Geometry geometry = reader.read(feature.getGeometry());

        final MapWritable properties = new MapWritable();

        feature.getProperties().entrySet().stream()
                .filter(e -> (outputColumns.size() == 0) || outputColumns.contains(e.getKey()))
                .forEach(e -> properties.put(new Text(e.getKey()), new Text(String.valueOf(e.getValue()))));

        if (geometry instanceof Polygon) {
            Polygon polygon = (Polygon) geometry;

            point(result, properties, polygon.getCentroid());
        } else if (geometry instanceof MultiPolygon) {
            for (int i = 0; i < geometry.getNumGeometries(); i++) {
                Geometry g = geometry.getGeometryN(i);
                if (g instanceof Polygon) {
                    Polygon polygon = (Polygon) g;
                    point(result, properties, polygon.getCentroid());
                }
            }
        } else if (geometry instanceof Point) {
            Point point = (Point) geometry;
            point(result, properties, point);
        } else if (geometry instanceof MultiPoint) {
            for (int i = 0; i < geometry.getNumGeometries(); i++) {
                Geometry g = geometry.getGeometryN(i);
                if (g instanceof Point) {
                    Point point = (Point) g;
                    point(result, properties, point);
                }
            }
        }
    }

    private void point(List<Point> result, MapWritable properties, Point source) {
        Point point = geometryFactory.createPoint(source.getCoordinate());
        MapWritable props = new MapWritable();

        props.putAll(properties);
        props.put(latAttr, new DoubleWritable(source.getY()));
        props.put(lonAttr, new DoubleWritable(source.getX()));
        if (defaultRadius != null) {
            props.put(radiusAttr, new DoubleWritable(defaultRadius));
        }

        point.setUserData(props);
        result.add(point);
    }
}