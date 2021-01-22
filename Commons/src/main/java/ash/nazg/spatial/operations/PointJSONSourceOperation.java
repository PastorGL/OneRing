/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.config.ConfigurationParameters;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.*;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class PointJSONSourceOperation extends Operation {
    @Description("By default, don't add _radius attribute to the point")
    public static final Double DEF_DEFAULT_RADIUS = null;

    public static final String VERB = "pointJsonSource";

    private String inputName;

    private String outputName;
    private List<String> outputColumns;
    private Double defaultRadius;

    @Override
    @Description("Take GeoJSON fragment file and produce a Point RDD")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_DEFAULT_RADIUS, Double.class, DEF_DEFAULT_RADIUS),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                true
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.inputs.get(0);

        outputName = describedProps.outputs.get(0);
        outputColumns = Arrays.asList(dataStreamsProps.outputColumns.get(outputName)).stream()
                .map(c -> c.replaceFirst("^[^.]+\\.", ""))
                .collect(Collectors.toList());

        defaultRadius = describedProps.defs.getTyped(OP_DEFAULT_RADIUS);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        Double _defaultRadius = defaultRadius;
        List<String> _outputColumns = outputColumns;

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<Point> output = ((JavaRDD<Object>) input.get(inputName))
                .flatMap(line -> {
                    List<Point> result = new ArrayList<>();
                    GeoJSONReader reader = new GeoJSONReader();

                    GeoJSON json = GeoJSONFactory.create(String.valueOf(line));

                    List<Feature> features = null;
                    if (json instanceof Feature) {
                        features = Collections.singletonList((Feature) json);
                    } else if (json instanceof FeatureCollection) {
                        features = Arrays.asList(((FeatureCollection) json).getFeatures());
                    }

                    if (features != null) {
                        Text latAttr = new Text(GEN_CENTER_LAT);
                        Text lonAttr = new Text(GEN_CENTER_LON);
                        Text radiusAttr = new Text(GEN_RADIUS);

                        for (Feature feature : features) {
                            Geometry geometry = reader.read(feature.getGeometry());

                            final MapWritable properties = new MapWritable();

                            feature.getProperties().entrySet().stream()
                                    .filter(e -> (_outputColumns.size() == 0) || _outputColumns.contains(e.getKey()))
                                    .forEach(e -> properties.put(new Text(e.getKey()), new Text(String.valueOf(e.getValue()))));

                            List<Point> points = new ArrayList<>();

                            if (geometry instanceof Polygon) {
                                points.add(geometry.getCentroid());
                            } else if (geometry instanceof MultiPolygon) {
                                for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                    points.add(geometry.getGeometryN(i).getCentroid());
                                }
                            } else if (geometry instanceof Point) {
                                points.add((Point) geometry);
                            } else if (geometry instanceof MultiPoint) {
                                for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                    points.add((Point) geometry.getGeometryN(i));
                                }
                            }

                            for (Point source : points) {
                                Point point = geometryFactory.createPoint(source.getCoordinate());
                                MapWritable props = new MapWritable();

                                props.putAll(properties);
                                props.put(latAttr, new DoubleWritable(source.getY()));
                                props.put(lonAttr, new DoubleWritable(source.getX()));
                                if (_defaultRadius != null) {
                                    props.put(radiusAttr, new DoubleWritable(_defaultRadius));
                                }

                                point.setUserData(props);
                                result.add(point);
                            }
                        }
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
