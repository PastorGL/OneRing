package ash.nazg.proximity.operations;

import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SpatialUtils;
import ash.nazg.config.OperationConfig;
import net.sf.geographiclib.Geodesic;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.proximity.config.ConfigurationParameters.*;

/**
 * Joining points by distance
 */
@SuppressWarnings("unused")
public class AreaCoversOperation extends Operation {
    public static final String VERB = "areaCovers";

    private String inputGeometriesName;
    private String inputSignalsName;

    private String outputName;

    @Override
    @Description("Takes a Point RDD and Polygon RDD and generates a Point RDD consisting" +
            " of all points that are contained inside the polygons")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_INPUT_SIGNALS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                        false
                                ),
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_INPUT_GEOMETRIES,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Polygon},
                                        false
                                ),
                        }
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_SIGNALS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                        false
                                ),
                        }
                )
        );
    }

    @Override
    public void setConfig(OperationConfig propertiesConfig) {
        super.setConfig(propertiesConfig);

        inputGeometriesName = describedProps.namedInputs.get(RDD_INPUT_GEOMETRIES);
        inputSignalsName = describedProps.namedInputs.get(RDD_INPUT_SIGNALS);

        outputName = describedProps.namedOutputs.get(RDD_OUTPUT_SIGNALS);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        String _inputGeometriesName = inputGeometriesName;
        String _inputSignalsName = inputSignalsName;

        JavaRDD<Polygon> geometriesInput = (JavaRDD<Polygon>) input.get(inputGeometriesName);

        Envelope maxEnvelope = geometriesInput
                .map(Geometry::getEnvelopeInternal)
                .max(new EnvelopeComparator());

        final double _maxRadius = Geodesic.WGS84.Inverse(
                (maxEnvelope.getMaxY() + maxEnvelope.getMinY()) / 2, (maxEnvelope.getMaxX() + maxEnvelope.getMinX()) / 2,
                maxEnvelope.getMaxY(), maxEnvelope.getMaxX()
        ).s12;

        final SpatialUtils spatialUtils = new SpatialUtils();

        JavaPairRDD<Long, Polygon> hashedGeometries = geometriesInput
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Long, Polygon>> result = new ArrayList<>();

                    Text latAttr = new Text("_center_lat");
                    Text lonAttr = new Text("_center_lon");

                    while (it.hasNext()) {
                        Polygon o = it.next();

                        MapWritable properties = (MapWritable) o.getUserData();

                        result.add(new Tuple2<>(
                                spatialUtils.getHash(
                                        ((DoubleWritable) properties.get(latAttr)).get(),
                                        ((DoubleWritable) properties.get(lonAttr)).get(),
                                        _maxRadius
                                ),
                                o)
                        );
                    }

                    return result.iterator();
                });


        JavaRDD<Point> inputSignals = (JavaRDD<Point>) input.get(inputSignalsName);

        Map<Long, Iterable<Polygon>> hashedGeometriesMap = hashedGeometries
                .groupByKey()
                .collectAsMap();

        // Broadcast hashed centroids
        Broadcast<HashMap<Long, Iterable<Polygon>>> broadcastHashedGeometries = ctx
                .broadcast(new HashMap<>(hashedGeometriesMap));

        final Set<Long> geometryHashes = new HashSet<>(hashedGeometriesMap.keySet());

        // Get hash coverage
        final Set<Long> geometryCoverage = geometryHashes.stream()
                .flatMap(hash -> spatialUtils.getNeighbours(hash, _maxRadius).stream())
                .collect(Collectors.toSet());

        final GeometryFactory geometryFactory = new GeometryFactory();

        // Filter signals by hash coverage
        JavaRDD<Point> signals = inputSignals
                .mapPartitions(it -> {
                    HashMap<Long, Iterable<Polygon>> geometries = broadcastHashedGeometries.getValue();

                    List<Point> result = new ArrayList<>();

                    Text latAttr = new Text("_center_lat");
                    Text lonAttr = new Text("_center_lon");

                    while (it.hasNext()) {
                        Point signal = it.next();

                        MapWritable signalProperties = (MapWritable) signal.getUserData();

                        double signalLat = ((DoubleWritable) signalProperties.get(latAttr)).get();
                        double signalLon = ((DoubleWritable) signalProperties.get(lonAttr)).get();
                        long signalHash = spatialUtils.getHash(
                                signalLat,
                                signalLon,
                                _maxRadius
                        );

                        if (geometryCoverage.contains(signalHash)) {
                            List<Long> neighbours = spatialUtils.getNeighbours(signalHash, _maxRadius);
                            neighbours.retainAll(geometryHashes);

                            for (long hash : neighbours) {
                                for (Polygon geometry : geometries.get(hash)) {
                                    if (signal.within(geometry)) {
                                        MapWritable properties = new MapWritable();
                                        ((MapWritable) geometry.getUserData()).forEach((k, v) -> properties.put(new Text(_inputGeometriesName + "." + k), new Text(String.valueOf(v))));
                                        properties.putAll(signalProperties);

                                        Point point = geometryFactory.createPoint(new Coordinate(signalLon, signalLat));
                                        point.setUserData(properties);
                                        result.add(point);
                                    }
                                }
                            }
                        }
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, signals);
    }

    public static class EnvelopeComparator implements Comparator<Envelope>, Serializable {
        public int compare(Envelope e1, Envelope e2) {
            double e1d = Geodesic.WGS84.Inverse(
                    (e1.getMaxY() + e1.getMinY()) / 2, (e1.getMaxX() + e1.getMinX()) / 2,
                    e1.getMaxY(), e1.getMaxX()
            ).s12;

            double e2d = Geodesic.WGS84.Inverse(
                    (e2.getMaxY() + e2.getMinY()) / 2, (e2.getMaxX() + e2.getMinX()) / 2,
                    e2.getMaxY(), e2.getMaxX()
            ).s12;

            return Double.compare(e1d, e2d);
        }
    }
}
