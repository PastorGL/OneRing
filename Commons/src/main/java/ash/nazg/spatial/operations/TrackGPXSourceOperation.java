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
import org.apache.commons.codec.Charsets;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.ByteArrayInputStream;
import java.util.*;

import static ash.nazg.spatial.config.ConfigurationParameters.GEN_TRACK_ID;
import static ash.nazg.spatial.config.ConfigurationParameters.GEN_USERID;

@SuppressWarnings("unused")
public class TrackGPXSourceOperation extends Operation {
    private String inputName;

    private String outputName;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("trackGpxSource", "Take GPX fragment file and produce a Track RDD",

                new PositionalStreamsMetaBuilder()
                        .ds("Plain RDD with GPS fragments per each line",
                                new StreamType[]{StreamType.Plain}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .ds("SegmentedTrack RDD",
                                new StreamType[]{StreamType.Track}, true
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
        JavaRDD<Object> rdd = (JavaRDD<Object>) input.get(inputName);

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<SegmentedTrack> output = rdd.flatMap(o -> {
            List<SegmentedTrack> result = new ArrayList<>();

            Text tsAttr = new Text("_ts");
            Text useridAttr = new Text(GEN_USERID);
            Text trackidAttr = new Text(GEN_TRACK_ID);

            String l = String.valueOf(o);

            GPX gpx = GPX.reader(GPX.Reader.Mode.LENIENT).read(new ByteArrayInputStream(l.getBytes(Charsets.UTF_8)));

            MapWritable props;
            for (Track g : gpx.getTracks()) {
                Text userid = new Text(g.getName().orElse(UUID.randomUUID().toString()));

                List<io.jenetics.jpx.TrackSegment> segments = g.getSegments();
                int segmentsSize = segments.size();
                TrackSegment[] ts = new TrackSegment[segmentsSize];

                for (int i = 0; i < segmentsSize; i++) {
                    io.jenetics.jpx.TrackSegment t = segments.get(i);

                    List<WayPoint> points = t.getPoints();
                    int pointsSize = points.size();
                    Point[] p = new Point[pointsSize];

                    for (int j = 0; j < pointsSize; j++) {
                        WayPoint wp = points.get(j);
                        Point pt = geometryFactory.createPoint(new Coordinate(wp.getLongitude().doubleValue(), wp.getLatitude().doubleValue()));

                        props = new MapWritable();
                        props.put(tsAttr, new DoubleWritable(wp.getTime().isPresent() ? wp.getTime().get().toEpochSecond() : j));
                        pt.setUserData(props);

                        p[j] = pt;
                    }

                    TrackSegment seg = new TrackSegment(p, geometryFactory);

                    props = new MapWritable();
                    props.put(trackidAttr, new Text(Integer.toString(i)));
                    props.put(useridAttr, userid);
                    seg.setUserData(props);

                    ts[i] = seg;
                }

                if (segmentsSize > 0) {
                    SegmentedTrack st = new SegmentedTrack(ts, geometryFactory);

                    props = new MapWritable();
                    props.put(useridAttr, userid);
                    st.setUserData(props);

                    result.add(st);
                }
            }

            return result.iterator();
        });

        return Collections.singletonMap(outputName, output);
    }
}
