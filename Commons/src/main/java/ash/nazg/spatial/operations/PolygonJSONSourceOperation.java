/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class PolygonJSONSourceOperation extends Operation {
    public static final String VERB = "polygonJsonSource";

    private String inputName;

    private String outputName;
    private List<String> outputColumns;

    @Override
    @Description("Take GeoJSON fragment file and produce a Polygon RDD")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.Plain},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.Polygon},
                                true
                        )
                )
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);

        outputName = opResolver.positionalOutput(0);
        String[] outputCols = dsResolver.outputColumns(outputName);
        outputColumns = (outputCols == null) ? Collections.emptyList() : Arrays.stream(outputCols)
                .map(c -> c.replaceFirst("^[^.]+\\.", ""))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final List<String> _outputColumns = outputColumns;

        JavaRDD<Polygon> output = ((JavaRDD<Object>) input.get(inputName))
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
                        Text latAttr = new Text(GEN_CENTER_LAT);
                        Text lonAttr = new Text(GEN_CENTER_LON);

                        for (Feature feature : features) {
                            Geometry geometry = reader.read(feature.getGeometry());

                            MapWritable properties = new MapWritable();

                            feature.getProperties().entrySet().stream()
                                    .filter(e -> _outputColumns.isEmpty() || _outputColumns.contains(e.getKey()))
                                    .forEach(e -> properties.put(new Text(e.getKey()), new Text(String.valueOf(e.getValue()))));

                            List<Geometry> geometries = new ArrayList<>();

                            if (geometry instanceof Polygon) {
                                geometries.add(geometry);
                            } else if (geometry instanceof MultiPolygon) {
                                for (int i = 0; i < geometry.getNumGeometries(); i++) {
                                    geometries.add(geometry.getGeometryN(i));
                                }
                            }

                            for (Geometry polygon : geometries) {
                                MapWritable props = new MapWritable();

                                props.putAll(properties);
                                Point centroid = polygon.getCentroid();
                                props.put(latAttr, new DoubleWritable(centroid.getY()));
                                props.put(lonAttr, new DoubleWritable(centroid.getX()));

                                polygon.setUserData(props);
                                result.add((Polygon) polygon);
                            }
                        }
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
