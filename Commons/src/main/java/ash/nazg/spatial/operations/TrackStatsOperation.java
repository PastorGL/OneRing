package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SegmentedTrack;
import ash.nazg.spatial.TrackSegment;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.util.*;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class TrackStatsOperation extends Operation {
    public static final String VERB = "trackStats";
    @Description("Track RDD to calculate the statistics")
    public static final String RDD_INPUT_TRACKS = "tracks";
    @Description("Optional Point RDD to pin tracks with same userid against")
    public static final String RDD_INPUT_PINS = "pins";

    private String inputName;
    private String outputName;

    private String pinsName;

    @Override
    @Description("Take a Track RDD and augment its properties with statistics")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(RDD_INPUT_PINS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                        false
                                ),
                                new TaskDescriptionLanguage.NamedStream(RDD_INPUT_TRACKS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
                                        false
                                )
                        }
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_TRACKS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Track},
                                        new String[]{GEN_USERID, GEN_POINTS, GEN_TRACKID, GEN_DURATION, GEN_RANGE, GEN_DISTANCE}
                                )
                        }
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.namedInputs.get(RDD_INPUT_TRACKS);
        pinsName = describedProps.namedInputs.get(RDD_INPUT_PINS);
        outputName = describedProps.namedOutputs.get(RDD_OUTPUT_TRACKS);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final boolean pinDistance = input.get(pinsName) != null;

        JavaPairRDD<Point, SegmentedTrack> inp;

        if (pinDistance) {
            JavaPairRDD<Text, Point> pins = ((JavaRDD<Point>) input.get(pinsName))
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Text, Point>> result = new ArrayList<>();

                        Text useridAttr = new Text("_userid");

                        while (it.hasNext()) {
                            Point next = it.next();

                            result.add(new Tuple2<>((Text) ((MapWritable) next.getUserData()).get(useridAttr), next));
                        }

                        return result.iterator();
                    });

            JavaPairRDD<Text, SegmentedTrack> tracks = ((JavaRDD<SegmentedTrack>) input.get(inputName))
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Text, SegmentedTrack>> result = new ArrayList<>();

                        Text useridAttr = new Text("_userid");

                        while (it.hasNext()) {
                            SegmentedTrack next = it.next();

                            result.add(new Tuple2<>((Text) ((MapWritable) next.getUserData()).get(useridAttr), next));
                        }

                        return result.iterator();
                    });

            inp = pins.join(tracks)
                    .mapToPair(Tuple2::_2);
        } else {
            inp = ((JavaRDD<SegmentedTrack>) input.get(inputName))
                    .mapToPair(t -> new Tuple2<>(null, t));
        }

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<SegmentedTrack> output = inp
                .mapPartitions(it -> {
                    List<SegmentedTrack> result = new ArrayList<>();

                    Text tsAttr = new Text("_ts");
                    Text durationAttr = new Text(GEN_DURATION);
                    Text distanceAttr = new Text(GEN_DISTANCE);
                    Text pointsAttr = new Text(GEN_POINTS);
                    Text rangeAttr = new Text(GEN_RANGE);

                    while (it.hasNext()) {
                        Tuple2<Point, SegmentedTrack> o = it.next();

                        SegmentedTrack trk = o._2;

                        Point start = null;
                        int numSegs = trk.getNumGeometries();
                        TrackSegment[] segs = new TrackSegment[numSegs];
                        int augPoints = 0;
                        double augDistance = 0.D;
                        double augRange = 0.D;
                        long augDuration = 0L;
                        for (int j = 0; j < numSegs; j++) {
                            TrackSegment augSeg;

                            TrackSegment seg = (TrackSegment) trk.getGeometryN(j);
                            Geometry[] wayPoints = seg.geometries();
                            int segPoints = wayPoints.length;
                            double segDistance = 0.D;
                            double segRange = 0.D;
                            long segDuration = 0L;

                            Point prev;
                            if (pinDistance) {
                                if (start == null) {
                                    start = o._1;
                                }
                                prev = (Point) wayPoints[0];
                            } else {
                                start = (Point) wayPoints[0];
                                prev = start;
                            }

                            for (int i = 1; i < segPoints; i++) {
                                Point point = (Point) wayPoints[i];

                                MapWritable props = (MapWritable) point.getUserData();
                                MapWritable prevProps = (MapWritable) prev.getUserData();

                                segDuration += ((DoubleWritable) props.get(tsAttr)).get() - ((DoubleWritable) prevProps.get(tsAttr)).get();
                                segDistance += Geodesic.WGS84.Inverse(prev.getY(), prev.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE).s12;
                                segRange = Math.max(segRange, Geodesic.WGS84.Inverse(start.getY(), start.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE).s12);
                                prev = point;
                            }

                            augSeg = new TrackSegment(wayPoints, geometryFactory);

                            augDuration += segDuration;
                            augDistance += segDistance;
                            augRange = Math.max(augRange, segRange);
                            augPoints += segPoints;

                            MapWritable segProps = (MapWritable) seg.getUserData();
                            segProps.put(durationAttr, new Text(String.valueOf(segDuration)));
                            segProps.put(distanceAttr, new Text(String.valueOf(segDistance)));
                            segProps.put(pointsAttr, new Text(String.valueOf(segPoints)));
                            segProps.put(rangeAttr, new Text(String.valueOf(segRange)));
                            augSeg.setUserData(segProps);

                            segs[j] = augSeg;
                        }

                        SegmentedTrack aug = new SegmentedTrack(segs, geometryFactory);

                        MapWritable augProps = (MapWritable) trk.getUserData();
                        augProps.put(durationAttr, new Text(String.valueOf(augDuration)));
                        augProps.put(distanceAttr, new Text(String.valueOf(augDistance)));
                        augProps.put(pointsAttr, new Text(String.valueOf(augPoints)));
                        augProps.put(rangeAttr, new Text(String.valueOf(augRange)));
                        aug.setUserData(augProps);

                        result.add(aug);
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }

}
