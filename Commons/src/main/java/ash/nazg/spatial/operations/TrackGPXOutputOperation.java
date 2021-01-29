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
import ash.nazg.spatial.TrackSegment;
import io.jenetics.jpx.GPX;
import io.jenetics.jpx.Track;
import io.jenetics.jpx.WayPoint;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.*;

import static ash.nazg.spatial.config.ConfigurationParameters.GEN_USERID;

@SuppressWarnings("unused")
public class TrackGPXOutputOperation extends Operation {
    public static final String VERB = "trackGpxOutput";

    private String inputName;
    private String outputName;

    @Override
    @Description("Take a Track RDD and produce a GPX fragment file")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
                                false
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Text> output = ((JavaRDD<SegmentedTrack>) input.get(inputName))
                .mapPartitions(it -> {
                    List<Text> result = new ArrayList<>();

                    final Text tsAttr = new Text("_ts");
                    final Text useridAttr = new Text(GEN_USERID);
                    GPX.Writer writer = GPX.writer();

                    while (it.hasNext()) {
                        SegmentedTrack trk = it.next();

                        GPX.Builder gpx = GPX.builder();
                        gpx.creator("OneRing");
                        Track.Builder trkBuilder = Track.builder();

                        for (Geometry g : trk) {
                            TrackSegment ts = (TrackSegment) g;
                            io.jenetics.jpx.TrackSegment.Builder segBuilder = io.jenetics.jpx.TrackSegment.builder();

                            for (Geometry gg : ts) {
                                Point p = (Point) gg;
                                WayPoint.Builder wp = WayPoint.builder();
                                wp.lat(p.getY());
                                wp.lon(p.getX());
                                wp.time((long) ((DoubleWritable) ((MapWritable) p.getUserData()).get(tsAttr)).get() * 1000L);

                                segBuilder.addPoint(wp.build());
                            }

                            trkBuilder.addSegment(segBuilder.build());
                        }

                        trkBuilder.name(((MapWritable) trk.getUserData()).get(useridAttr).toString());
                        gpx.addTrack(trkBuilder.build());

                        result.add(new Text(writer.toString(gpx.build())));
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
