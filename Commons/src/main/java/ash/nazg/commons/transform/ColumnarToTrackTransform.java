/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.metadata.TransformMeta;
import ash.nazg.metadata.TransformedStreamMetaBuilder;
import ash.nazg.spatial.TrackComparator;
import ash.nazg.spatial.TrackPartitioner;
import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.data.*;
import ash.nazg.data.spatial.PointEx;
import ash.nazg.data.spatial.SegmentedTrack;
import ash.nazg.data.spatial.TrackSegment;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;
import scala.Tuple4;

import java.util.*;

@SuppressWarnings("unused")
public class ColumnarToTrackTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToTrack", StreamType.Columnar, StreamType.Track,
                "Take a Columnar DataStream and create Segmented Tracks from it",

                new DefinitionMetaBuilder()
                        .def("lat.column", "Point latitude column")
                        .def("lon.column", "Point longitude column")
                        .def("ts.column", "Point time stamp column")
                        .def("userid.column", "Point User ID column")
                        .def("trackid.column", "Optional Point track segment ID column",
                                null, "By default, create single-segmented tracks")
                        .build(),
                new TransformedStreamMetaBuilder()
                        .genCol("_userid", "User ID property of Tracks and Segments")
                        .genCol("_trackid", "Track ID property of Segmented Tracks")
                        .genCol("_ts", "Time stamp of a Point")
                        .build()

        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final String _latColumn = params.get("lat.column");
            final String _lonColumn = params.get("lon.column");
            final String _tsColumn = params.get("ts.column");
            final String _useridColumn = params.get("userid.column");
            final String _trackColumn = params.get("trackid.column");

            List<String> pointColumns = newColumns.get("point");
            if (pointColumns == null) {
                pointColumns = ds.accessor.attributes("value");
            }
            final List<String> _pointColumns = pointColumns;

            JavaRDD<Columnar> signalsInput = (JavaRDD<Columnar>) ds.get();
            int _numPartitions = signalsInput.getNumPartitions();

            final boolean isSegmented = (_trackColumn != null);

            JavaPairRDD<Tuple2<String, Double>, Tuple4<Double, Double, String, Columnar>> signals = signalsInput
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Tuple2<String, Double>, Tuple4<Double, Double, String, Columnar>>> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Columnar row = it.next();

                            String userId = row.asString(_useridColumn);
                            Double lat = row.asDouble(_latColumn);
                            Double lon = row.asDouble(_lonColumn);
                            Double timestamp = row.asDouble(_tsColumn);

                            String track = isSegmented ? row.asString(_trackColumn) : null;

                            ret.add(new Tuple2<>(new Tuple2<>(userId, timestamp), new Tuple4<>(lat, lon, track, row)));
                        }

                        return ret.iterator();
                    })
                    .repartitionAndSortWithinPartitions(new TrackPartitioner(_numPartitions), new TrackComparator<>()) // pre-sort by timestamp
                    ;

            HashMap<Integer, Integer> useridCountPerPartition = new HashMap<>(signals
                    .mapPartitionsWithIndex((idx, it) -> {
                        List<Tuple2<Integer, Integer>> num = new ArrayList<>();

                        Set<String> userids = new HashSet<>();
                        while (it.hasNext()) {
                            String userid = it.next()._1._1;
                            userids.add(userid);
                        }

                        num.add(new Tuple2<>(idx, userids.size()));

                        return num.iterator();
                    }, true)
                    .mapToPair(t -> t)
                    .collectAsMap()
            );

            Broadcast<HashMap<Integer, Integer>> num = JavaSparkContext.fromSparkContext(signalsInput.context()).broadcast(useridCountPerPartition);

            final GeometryFactory geometryFactory = new GeometryFactory();

            JavaRDD<SegmentedTrack> output = signals.mapPartitionsWithIndex((idx, it) -> {
                int useridCount = num.getValue().get(idx);

                Map<String, Integer> useridOrd = new HashMap<>();

                String[] userids = new String[useridCount];
                List<Map<String, Object>>[] allSegProps = new List[useridCount];
                List<List<PointEx>>[] allPoints = new List[useridCount];
                int n = 0;
                while (it.hasNext()) {
                    Tuple2<Tuple2<String, Double>, Tuple4<Double, Double, String, Columnar>> line = it.next();

                    String userid = line._1._1;
                    int current;
                    if (useridOrd.containsKey(userid)) {
                        current = useridOrd.get(userid);
                    } else {
                        useridOrd.put(userid, n);
                        userids[n] = userid;
                        current = n;

                        n++;
                    }

                    List<Map<String, Object>> segProps = allSegProps[current];
                    List<List<PointEx>> trackPoints = allPoints[current];
                    if (segProps == null) {
                        segProps = new ArrayList<>();
                        allSegProps[current] = segProps;
                        trackPoints = new ArrayList<>();
                        allPoints[current] = trackPoints;
                    }

                    List<PointEx> segPoints;
                    String trackId;
                    if (isSegmented) {
                        trackId = line._2._3();

                        String lastTrackId = null;
                        Map<String, Object> lastSegment;
                        if (segProps.size() != 0) {
                            lastSegment = segProps.get(segProps.size() - 1);
                            lastTrackId = lastSegment.get("_trackid").toString();
                        }

                        if (trackId.equals(lastTrackId)) {
                            segPoints = trackPoints.get(trackPoints.size() - 1);
                        } else {
                            Map<String, Object> props = new HashMap<>();
                            props.put("_userid", userid);
                            props.put("_trackid", trackId);

                            segProps.add(props);
                            segPoints = new ArrayList<>();
                            trackPoints.add(segPoints);
                        }
                    } else {
                        if (segProps.size() == 0) {
                            Map<String, Object> props = new HashMap<>();
                            props.put("_userid", userid);

                            segProps.add(props);
                            segPoints = new ArrayList<>();
                            trackPoints.add(segPoints);
                        } else {
                            segPoints = trackPoints.get(0);
                        }
                    }

                    PointEx point = new PointEx(geometryFactory.createPoint(new Coordinate(line._2._2(), line._2._1())));
                    Map<String, Object> pointProps = new HashMap<>();
                    Columnar row = line._2._4();
                    for (String col : _pointColumns) {
                        pointProps.put(col, row.asIs(col));
                    }
                    pointProps.put("_ts", line._1._2);
                    point.setUserData(pointProps);

                    segPoints.add(point);
                }

                List<SegmentedTrack> result = new ArrayList<>(useridCount);

                for (n = 0; n < useridCount; n++) {
                    String userid = userids[n];

                    List<List<PointEx>> points = allPoints[n];
                    TrackSegment[] segments = new TrackSegment[points.size()];
                    for (int i = 0; i < points.size(); i++) {
                        List<PointEx> segPoints = points.get(i);
                        segments[i] = new TrackSegment(segPoints.toArray(new PointEx[0]), geometryFactory);
                        segments[i].setUserData(allSegProps[n].get(i));
                    }

                    SegmentedTrack trk = new SegmentedTrack(segments, geometryFactory);

                    Map<String, Object> props = new HashMap<>();
                    props.put("_userid", userid);
                    trk.setUserData(props);

                    result.add(trk);
                }

                return result.iterator();
            }, true);

            Map<String, List<String>> outputColumns = new HashMap<>();
            outputColumns.put("track", Collections.singletonList("_userid"));
            List<String> segmentProps = new ArrayList<>();
            segmentProps.add("_userid");
            if (isSegmented) {
                segmentProps.add("_trackid");
            }
            outputColumns.put("segment", segmentProps);
            List<String> pointProps = new ArrayList<>(_pointColumns);
            pointProps.add("_ts");
            outputColumns.put("point", pointProps);
            return new DataStream(StreamType.Track, output, outputColumns);
        };
    }
}
