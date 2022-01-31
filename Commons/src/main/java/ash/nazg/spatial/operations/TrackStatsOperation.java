/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionEnum;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.NamedStreamsMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SegmentedTrack;
import ash.nazg.spatial.TrackSegment;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class TrackStatsOperation extends Operation {
    public static final String RDD_INPUT_TRACKS = "tracks";
    public static final String RDD_INPUT_PINS = "pins";
    public static final String OP_PINNING_MODE = "pinning.mode";

    private String tracksName;
    private String pinsName;

    private String outputName;

    private PinningMode pinningMode;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("trackStats", "Take a Track RDD and augment its Points', TrackSegments' and SegmentedTracks' properties with statistics",

                new NamedStreamsMetaBuilder()
                        .ds(RDD_INPUT_TRACKS, "SegmentedTrack RDD to calculate the statistics",
                                new StreamType[]{StreamType.Track}
                        )
                        .ds(RDD_INPUT_PINS, "Optional Point RDD to pin tracks with same _userid property against (for pinning.mode=INPUT_PINS)",
                                new StreamType[]{StreamType.Point}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_PINNING_MODE, "Track pinning mode for _radius calculation", PinningMode.class,
                                PinningMode.INPUT_PINS.name(), "By default, pin to points supplied by an external input")
                        .build(),

                new NamedStreamsMetaBuilder()
                        .ds(RDD_OUTPUT_TRACKS, "SegmentedTrack output RDD with stats",
                                new StreamType[]{StreamType.Track}, true
                        )
                        .genCol(RDD_OUTPUT_TRACKS, GEN_POINTS, "Number of Track or Segment points")
                        .genCol(RDD_OUTPUT_TRACKS, GEN_DURATION, "Track or Segment duration, seconds")
                        .genCol(RDD_OUTPUT_TRACKS, GEN_RADIUS, "Track or Segment max distance from its pinning point, meters")
                        .genCol(RDD_OUTPUT_TRACKS, GEN_DISTANCE, "Track or Segment length, meters")
                        .genCol(RDD_OUTPUT_TRACKS, GEN_AZI_FROM_PREV, "Point azimuth from the previous point")
                        .genCol(RDD_OUTPUT_TRACKS, GEN_AZI_TO_PREV, "Point azimuth to the previous point")
                        .genCol(RDD_OUTPUT_TRACKS, GEN_AZI_TO_NEXT, "Point azimuth to the next point")
                        .genCol(RDD_OUTPUT_TRACKS, GEN_AZI_FROM_NEXT, "Point azimuth from the next point")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        tracksName = opResolver.namedInput(RDD_INPUT_TRACKS);
        pinsName = opResolver.namedInput(RDD_INPUT_PINS);

        outputName = opResolver.namedOutput(RDD_OUTPUT_TRACKS);

        pinningMode = opResolver.definition(OP_PINNING_MODE);
    }

    @SuppressWarnings("rawtypes")
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

            JavaPairRDD<Text, SegmentedTrack> tracks = ((JavaRDD<SegmentedTrack>) input.get(tracksName))
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
            inp = ((JavaRDD<SegmentedTrack>) input.get(tracksName))
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
                    Text aziToPrev = new Text(GEN_AZI_TO_PREV);
                    Text aziFromPrev = new Text(GEN_AZI_FROM_PREV);
                    Text aziToNext = new Text(GEN_AZI_TO_NEXT);
                    Text aziFromNext = new Text(GEN_AZI_FROM_NEXT);

                    while (it.hasNext()) {
                        Tuple2<Point, SegmentedTrack> o = it.next();

                        SegmentedTrack trk = o._2;

                        Point trkPin = null;
                        Point segPin = null;
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

                            switch (_pinningMode) {
                                case SEGMENT_CENTROIDS: {
                                    if (j == 0) {
                                        trkPin = trk.getCentroid();
                                    }
                                    segPin = seg.getCentroid();
                                    break;
                                }
                                case TRACK_CENTROIDS: {
                                    if (j == 0) {
                                        trkPin = trk.getCentroid();
                                        segPin = trkPin;
                                    }
                                    break;
                                }
                                case SEGMENT_STARTS: {
                                    segPin = (Point) wayPoints[0];
                                    if (j == 0) {
                                        trkPin = segPin;
                                    }
                                    break;
                                }
                                case TRACK_STARTS: {
                                    if (j == 0) {
                                        trkPin = (Point) wayPoints[0];
                                        segPin = trkPin;
                                    }
                                    break;
                                }
                                default: {
                                    if (j == 0) {
                                        trkPin = o._1;
                                        segPin = trkPin;
                                    }
                                    break;
                                }
                            }

                            double pntRadius;
                            Point prev = (Point) wayPoints[0];
                            for (int i = 0; i < segPoints; i++) {
                                Geometry wayPoint = wayPoints[i];
                                Point point = (Point) wayPoint;

                                MapWritable props = (MapWritable) point.getUserData();
                                MapWritable prevProps = (MapWritable) prev.getUserData();

                                segDuration += ((DoubleWritable) props.get(tsAttr)).get() - ((DoubleWritable) prevProps.get(tsAttr)).get();
                                GeodesicData inverse = Geodesic.WGS84.Inverse(prev.getY(), prev.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE | GeodesicMask.AZIMUTH);
                                segDistance += inverse.s12;

                                if (i != 0) {
                                    props.put(aziFromPrev, new Text(String.valueOf(inverse.azi2)));
                                    props.put(aziToPrev, new Text(String.valueOf(inverse.azi1)));
                                    prevProps.put(aziFromNext, new Text(String.valueOf(inverse.azi1)));
                                    prevProps.put(aziToNext, new Text(String.valueOf(inverse.azi2)));
                                }

                                pntRadius = Geodesic.WGS84.Inverse(segPin.getY(), segPin.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE).s12;
                                props.put(radiusAttr, new Text(String.valueOf(pntRadius)));
                                segRadius = Math.max(segRadius, pntRadius);

                                augRadius = Math.max(augRadius, Geodesic.WGS84.Inverse(trkPin.getY(), trkPin.getX(),
                                        point.getY(), point.getX(), GeodesicMask.DISTANCE).s12);

                                if ((_pinningMode == PinningMode.SEGMENT_CENTROIDS) || (_pinningMode == PinningMode.SEGMENT_STARTS)) {
                                    props.put(durationAttr, new Text(String.valueOf(segDuration)));
                                    props.put(distanceAttr, new Text(String.valueOf(segDistance)));
                                    props.put(pointsAttr, new Text(String.valueOf(i + 1)));
                                } else {
                                    props.put(durationAttr, new Text(String.valueOf(augDuration + segDuration)));
                                    props.put(distanceAttr, new Text(String.valueOf(augDistance + segDistance)));
                                    props.put(pointsAttr, new Text(String.valueOf(augPoints + i + 1)));
                                }

                                prev = point;
                            }

                            augSeg = new TrackSegment(wayPoints, geometryFactory);

                            augDuration += segDuration;
                            augDistance += segDistance;
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

    public enum PinningMode implements DefinitionEnum {
        SEGMENT_CENTROIDS("Pin TrackSegments by their own centroids"),
        TRACK_CENTROIDS("Pin TrackSegments by parent SegmentedTrack centroid"),
        SEGMENT_STARTS("Pin TrackSegments by their own starting points"),
        TRACK_STARTS("Pin TrackSegments by parent SegmentedTrack starting point"),
        INPUT_PINS("Pin both SegmentedTracks and TrackSegments by externally supplied pin points");

        private final String descr;

        PinningMode(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
