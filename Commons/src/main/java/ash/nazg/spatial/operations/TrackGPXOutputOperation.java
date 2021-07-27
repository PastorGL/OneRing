/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ash.nazg.spatial.config.ConfigurationParameters.GEN_USERID;

@SuppressWarnings("unused")
public class TrackGPXOutputOperation extends Operation {
    private String inputName;

    private String outputName;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("trackGpxOutput", "Take a Track RDD and produce a GPX fragment file",

                new PositionalStreamsMetaBuilder()
                        .ds("SegmentedTrack RDD",
                                new StreamType[]{StreamType.Track}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .ds("Plain RDD with GPX fragments per each line",
                                new StreamType[]{StreamType.Plain}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);
    }

    @SuppressWarnings("rawtypes")
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
