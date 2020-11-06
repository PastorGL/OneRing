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
    @Description("Optional Point RDD to pin tracks with same _userid property against (for pinning.mode=INPUT_PINS)")
    public static final String RDD_INPUT_PINS = "pins";
    @Description("Track pinning mode for _radius calculation")
    public static final String OP_PINNING_MODE = "pinning.mode";
    @Description("By default, pin to points supplied by an external input")
    public static final PinningMode DEF_PINNING_MODE = PinningMode.INPUT_PINS;

    private String inputName;
    private String outputName;

    private String pinsName;
    private PinningMode pinningMode;

    @Override
    @Description("Take a Track RDD and augment its properties with statistics")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_PINNING_MODE, PinningMode.class, DEF_PINNING_MODE),
                },

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
                                        new String[]{GEN_USERID, GEN_POINTS, GEN_TRACKID, GEN_DURATION, GEN_RADIUS, GEN_DISTANCE}
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

        pinningMode = describedProps.defs.getTyped(OP_PINNING_MODE);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaPairRDD<Point, SegmentedTrack> inp;

        if (pinningMode == PinningMode.INPUT_PINS) {
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

        PinningMode _pinningMode = pinningMode;

        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<SegmentedTrack> output = inp
                .mapPartitions(it -> {
                    List<SegmentedTrack> result = new ArrayList<>();

                    Text tsAttr = new Text("_ts");
                    Text durationAttr = new Text(GEN_DURATION);
                    Text distanceAttr = new Text(GEN_DISTANCE);
                    Text pointsAttr = new Text(GEN_POINTS);
                    Text radiusAttr = new Text(GEN_RADIUS);

                    while (it.hasNext()) {
                        Tuple2<Point, SegmentedTrack> o = it.next();

                        SegmentedTrack trk = o._2;

                        Point pin = null;
                        int numSegs = trk.getNumGeometries();
                        TrackSegment[] segs = new TrackSegment[numSegs];
                        int augPoints = 0;
                        double augDistance = 0.D;
                        double augRadius = 0.D;
                        long augDuration = 0L;
                        for (int j = 0; j < numSegs; j++) {
                            TrackSegment augSeg;

                            TrackSegment seg = (TrackSegment) trk.getGeometryN(j);
                            Geometry[] wayPoints = seg.geometries();
                            int segPoints = wayPoints.length;
                            double segDistance = 0.D;
                            double segRadius = 0.D;
                            long segDuration = 0L;

                            Point prev = (Point) wayPoints[0];
                            switch (_pinningMode) {
                                case SEGMENT_CENTROIDS: {
                                    pin = seg.getCentroid();
                                    break;
                                }
                                case TRACK_CENTROIDS: {
                                    if (j == 0) {
                                        pin = trk.getCentroid();
                                    }
                                    break;
                                }
                                case SEGMENT_STARTS: {
                                    pin = (Point) wayPoints[0];
                                    break;
                                }
                                case TRACK_STARTS: {
                                    if (j == 0) {
                                        pin = (Point) wayPoints[0];
                                    }
                                    break;
                                }
                                default: {
                                    pin = o._1;
                                    break;
                                }
                            }

                            for (int i = 1; i < segPoints; i++) {
                                Point point = (Point) wayPoints[i];

                                MapWritable props = (MapWritable) point.getUserData();
                                MapWritable prevProps = (MapWritable) prev.getUserData();

                                segDuration += ((DoubleWritable) props.get(tsAttr)).get() - ((DoubleWritable) prevProps.get(tsAttr)).get();
                                segDistance += Geodesic.WGS84.Inverse(prev.getY(), prev.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE).s12;
                                segRadius = Math.max(segRadius, Geodesic.WGS84.Inverse(pin.getY(), pin.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE).s12);
                                prev = point;
                            }

                            augSeg = new TrackSegment(wayPoints, geometryFactory);

                            augDuration += segDuration;
                            augDistance += segDistance;
                            augRadius = Math.max(augRadius, segRadius);
                            augPoints += segPoints;

                            MapWritable segProps = (MapWritable) seg.getUserData();
                            segProps.put(durationAttr, new Text(String.valueOf(segDuration)));
                            segProps.put(distanceAttr, new Text(String.valueOf(segDistance)));
                            segProps.put(pointsAttr, new Text(String.valueOf(segPoints)));
                            segProps.put(radiusAttr, new Text(String.valueOf(segRadius)));
                            augSeg.setUserData(segProps);

                            segs[j] = augSeg;
                        }

                        SegmentedTrack aug = new SegmentedTrack(segs, geometryFactory);

                        MapWritable augProps = (MapWritable) trk.getUserData();
                        augProps.put(durationAttr, new Text(String.valueOf(augDuration)));
                        augProps.put(distanceAttr, new Text(String.valueOf(augDistance)));
                        augProps.put(pointsAttr, new Text(String.valueOf(augPoints)));
                        augProps.put(radiusAttr, new Text(String.valueOf(augRadius)));
                        aug.setUserData(augProps);

                        result.add(aug);
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }

    public enum PinningMode {
        @Description("Pin TrackSegments by their own centroids")
        SEGMENT_CENTROIDS,
        @Description("Pin TrackSegments by parent SegmentedTrack centroid")
        TRACK_CENTROIDS,
        @Description("Pin TrackSegments by their own starting points")
        SEGMENT_STARTS,
        @Description("Pin TrackSegments by parent SegmentedTrack starting point")
        TRACK_STARTS,
        @Description("Pin both SegmentedTracks and TrackSegments by externally supplied pin points")
        INPUT_PINS
    }
}
