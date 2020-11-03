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

import static ash.nazg.spatial.config.ConfigurationParameters.GEN_TRACKID;
import static ash.nazg.spatial.config.ConfigurationParameters.GEN_USERID;

@SuppressWarnings("unused")
public class TrackGPXSourceOperation extends Operation {
    public static final String VERB = "trackGpxSource";

    private String inputName;
    private String outputName;

    @Override
    @Description("Take GPX fragment file and produce a Track RDD")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
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
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Object> rdd = (JavaRDD<Object>) input.get(inputName);

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<SegmentedTrack> output = rdd.flatMap(o -> {
            List<SegmentedTrack> result = new ArrayList<>();

            Text tsAttr = new Text("_ts");
            Text useridAttr = new Text(GEN_USERID);
            Text trackidAttr = new Text(GEN_TRACKID);

            String l = String.valueOf(o);

            GPX gpx = GPX.reader(GPX.Reader.Mode.LENIENT).read(new ByteArrayInputStream(l.getBytes(Charsets.UTF_8)));

            MapWritable props;
            for (Track g : gpx.getTracks()) {
                List<TrackSegment> ts = new ArrayList<>();

                List<io.jenetics.jpx.TrackSegment> segments = g.getSegments();
                for (int i = 0, segmentsSize = segments.size(); i < segmentsSize; i++) {
                    List<Point> p = new ArrayList<>();

                    io.jenetics.jpx.TrackSegment t = segments.get(i);
                    for (WayPoint wp : t.getPoints()) {
                        Point pt = geometryFactory.createPoint(new Coordinate(wp.getLongitude().doubleValue(), wp.getLatitude().doubleValue()));

                        props = new MapWritable();
                        props.put(tsAttr, new DoubleWritable(wp.getTime().get().toEpochSecond()));
                        pt.setUserData(props);

                        p.add(pt);
                    }

                    TrackSegment seg = new TrackSegment(p.toArray(new Point[0]), geometryFactory);

                    props = new MapWritable();
                    props.put(trackidAttr, new Text(Integer.toString(i)));
                    seg.setUserData(props);

                    ts.add(seg);
                }

                SegmentedTrack st = new SegmentedTrack(ts.toArray(new TrackSegment[0]), geometryFactory);

                props = new MapWritable();
                props.put(useridAttr, new Text(g.getName().get()));
                st.setUserData(props);

                result.add(st);
            }

            return result.iterator();
        });

        return Collections.singletonMap(outputName, output);
    }
}
