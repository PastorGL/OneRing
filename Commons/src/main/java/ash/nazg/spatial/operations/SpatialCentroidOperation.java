/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
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

import java.util.*;

import static ash.nazg.spatial.config.ConfigurationParameters.GEN_CENTER_LAT;
import static ash.nazg.spatial.config.ConfigurationParameters.GEN_CENTER_LON;

@SuppressWarnings("unused")
public class SpatialCentroidOperation extends Operation {
    @Description("What to output for SegmentedTrack RDDs")
    public static final String OP_OUTPUT_MODE = "tracks.mode";
    @Description("By default, output both SegmentedTrack's and TrackSegment's data")
    public static final OutputMode DEF_OUTPUT_MODE = OutputMode.BOTH;

    public static final String VERB = "spatialCentroid";

    private String inputName;
    private String outputName;

    private OutputMode outputMode;

    @Override
    @Description("Take a SegmentedTrack or Polygon RDD and extract a Point RDD of centroids while keeping" +
            " all other properties")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_OUTPUT_MODE, OutputMode.class, DEF_OUTPUT_MODE)
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Polygon, TaskDescriptionLanguage.StreamType.Track},
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

        outputMode = describedProps.defs.getTyped(OP_OUTPUT_MODE);
    }

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

    public enum OutputMode {
        @Description("Output only TrackSegments' centroids")
        SEGMENTS,
        @Description("Output only SegmentedTracks' centroids")
        TRACKS,
        @Description("Output both SegmentedTracks' and then each of their TrackSegments' centroids")
        BOTH
    }
}
