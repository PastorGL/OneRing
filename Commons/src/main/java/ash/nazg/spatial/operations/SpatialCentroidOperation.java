/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionEnum;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SegmentedTrack;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ash.nazg.spatial.config.ConfigurationParameters.GEN_CENTER_LAT;
import static ash.nazg.spatial.config.ConfigurationParameters.GEN_CENTER_LON;

@SuppressWarnings("unused")
public class SpatialCentroidOperation extends Operation {
    public static final String OP_OUTPUT_MODE = "tracks.mode";

    private String inputName;

    private String outputName;

    private OutputMode outputMode;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("spatialCentroid", "Take a SegmentedTrack or Polygon RDD and extract a Point RDD of centroids while keeping" +
                " all other properties",

                new PositionalStreamsMetaBuilder()
                        .ds("SegmentedTrack or Polygon RDD",
                                new StreamType[]{StreamType.Polygon, StreamType.Track}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_OUTPUT_MODE, "What to output for SegmentedTrack RDDs", OutputMode.class,
                                OutputMode.BOTH.name(), "By default, output both SegmentedTrack's and TrackSegment's data")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Point RDD with centroids",
                                new StreamType[]{StreamType.Point}, true
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);

        outputMode = opResolver.definition(OP_OUTPUT_MODE);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final OutputMode _outputMode = outputMode;

        JavaRDD<Point> output = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(it -> {
                    Text latAttr = new Text(GEN_CENTER_LAT);
                    Text lonAttr = new Text(GEN_CENTER_LON);

                    List<Point> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Geometry g = (Geometry) it.next();

                        if (g instanceof Polygon) {
                            Point centroid = g.getCentroid();

                            MapWritable props = new MapWritable((MapWritable) g.getUserData());
                            props.put(latAttr, new DoubleWritable(centroid.getY()));
                            props.put(lonAttr, new DoubleWritable(centroid.getX()));

                            centroid.setUserData(props);
                            ret.add(centroid);
                        } else {
                            MapWritable trackProps = (MapWritable) g.getUserData();

                            if (_outputMode != OutputMode.SEGMENTS) {
                                Point centroid = g.getCentroid();

                                MapWritable props = new MapWritable(trackProps);
                                props.put(latAttr, new DoubleWritable(centroid.getY()));
                                props.put(lonAttr, new DoubleWritable(centroid.getX()));

                                centroid.setUserData(props);
                                ret.add(centroid);
                            }

                            if (_outputMode != OutputMode.TRACKS) {
                                for (Geometry gg : ((SegmentedTrack) g).geometries()) {
                                    Point centroid = gg.getCentroid();

                                    MapWritable props = new MapWritable(trackProps);
                                    props.putAll((MapWritable) gg.getUserData());
                                    props.put(latAttr, new DoubleWritable(centroid.getY()));
                                    props.put(lonAttr, new DoubleWritable(centroid.getX()));

                                    centroid.setUserData(props);
                                    ret.add(centroid);
                                }
                            }
                        }
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }

    public enum OutputMode implements DefinitionEnum {
        SEGMENTS("Output only TrackSegments' centroids"),
        TRACKS("Output only SegmentedTracks' centroids"),
        BOTH("Output both SegmentedTracks' and then each of their TrackSegments' centroids");

        private final String descr;

        OutputMode(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
