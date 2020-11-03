/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.proximity.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.spatial.SpatialUtils;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.proximity.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class ProximityFilterOperation extends Operation {
    public static final String VERB = "proximityFilter";

    private String inputSignalsName;
    private String inputPoisName;

    private String outputSignalsName;
    private String outputEvictedName;

    @Override
    @Description("Takes a Point RDD and POI Point RDD and generates a Point RDD consisting" +
            " of all points that are within the range of POIs")
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
                                        RDD_INPUT_POIS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                        false
                                ),
                        }
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_OUTPUT_SIGNALS,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                        false
                                ),
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_EVICTED,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                        false
                                ),
                        }
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputSignalsName = describedProps.namedInputs.get(RDD_INPUT_SIGNALS);
        inputPoisName = describedProps.namedInputs.get(RDD_INPUT_POIS);

        outputSignalsName = describedProps.namedOutputs.get(RDD_OUTPUT_SIGNALS);
        outputEvictedName = describedProps.namedOutputs.get(RDD_OUTPUT_EVICTED);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final String _inputSignalsName = inputSignalsName;
        final String _inputPoisName = inputPoisName;

        JavaRDD<Point> inputSignals = (JavaRDD<Point>) input.get(inputSignalsName);

        JavaRDD<Point> inputPois = (JavaRDD<Point>) input.get(inputPoisName);

        final SpatialUtils spatialUtils = new SpatialUtils();

        // Get POIs radii
        JavaRDD<Tuple2<MapWritable, Double>> poiRadii = inputPois
                .mapPartitions(it -> {
                    List<Tuple2<MapWritable, Double>> result = new ArrayList<>();

                    Text radiusAttr = new Text("_radius");

                    while (it.hasNext()) {
                        Point o = it.next();

                        MapWritable properties = (MapWritable) o.getUserData();

                        double radius = ((DoubleWritable) properties.get(radiusAttr)).get();
                        result.add(new Tuple2<>(properties, radius));
                    }

                    return result.iterator();
                });

        final double _maxRadius = poiRadii
                .map(t -> t._2)
                .max(Comparator.naturalOrder());

        // hash -> props, radius
        JavaPairRDD<Long, Tuple2<MapWritable, Double>> hashedPois = poiRadii
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Long, Tuple2<MapWritable, Double>>> result = new ArrayList<>();

                    Text latAttr = new Text("_center_lat");
                    Text lonAttr = new Text("_center_lon");

                    while (it.hasNext()) {
                        Tuple2<MapWritable, Double> o = it.next();

                        MapWritable properties = o._1;

                        result.add(new Tuple2<>(
                                spatialUtils.getHash(
                                        ((DoubleWritable) properties.get(latAttr)).get(),
                                        ((DoubleWritable) properties.get(lonAttr)).get(),
                                        _maxRadius
                                ),
                                new Tuple2<>(properties, o._2))
                        );
                    }

                    return result.iterator();
                });

        Map<Long, Iterable<Tuple2<MapWritable, Double>>> hashedPoisMap = hashedPois
                .groupByKey()
                .collectAsMap();

        // Broadcast hashed POIs
        Broadcast<HashMap<Long, Iterable<Tuple2<MapWritable, Double>>>> broadcastHashedPois = ctx
                .broadcast(new HashMap<>(hashedPoisMap));

        final Set<Long> poiHashes = new HashSet<>(hashedPoisMap.keySet());

        // Get hash coverage
        final Set<Long> poiCoverage = poiHashes.stream()
                .flatMap(hash -> spatialUtils.getNeighbours(hash, _maxRadius).stream())
                .collect(Collectors.toSet());

        final GeometryFactory geometryFactory = new GeometryFactory();

        // Filter signals by hash coverage
        JavaPairRDD<Boolean, Point> signals = inputSignals
                .mapPartitionsToPair(it -> {
                    HashMap<Long, Iterable<Tuple2<MapWritable, Double>>> pois = broadcastHashedPois.getValue();

                    List<Tuple2<Boolean, Point>> result = new ArrayList<>();

                    Text latAttr = new Text("_center_lat");
                    Text lonAttr = new Text("_center_lon");
                    Text distanceAttr = new Text("_distance");

                    while (it.hasNext()) {
                        Point signal = it.next();
                        boolean added = false;

                        MapWritable signalProperties = (MapWritable) signal.getUserData();

                        double signalLat = ((DoubleWritable) signalProperties.get(latAttr)).get();
                        double signalLon = ((DoubleWritable) signalProperties.get(lonAttr)).get();
                        long signalHash = spatialUtils.getHash(
                                signalLat,
                                signalLon,
                                _maxRadius
                        );

                        if (poiCoverage.contains(signalHash)) {
                            List<Long> neighbours = spatialUtils.getNeighbours(signalHash, _maxRadius);
                            neighbours.retainAll(poiHashes);

                            for (long hash : neighbours) {
                                for (Tuple2<MapWritable, Double> poi : pois.get(hash)) {
                                    MapWritable poiProperties = poi._1;

                                    double pLat = ((DoubleWritable) poiProperties.get(latAttr)).get();
                                    double pLon = ((DoubleWritable) poiProperties.get(lonAttr)).get();

                                    double distance = Geodesic.WGS84.Inverse(signalLat, signalLon, pLat, pLon, GeodesicMask.DISTANCE).s12;

                                    //check if poi falls into radius
                                    if (distance <= poi._2) {
                                        MapWritable properties = new MapWritable();
                                        poiProperties.forEach((k, v) -> properties.put(new Text(_inputPoisName + "." + k), new Text(String.valueOf(v))));
                                        properties.putAll(signalProperties);
                                        properties.put(distanceAttr, new DoubleWritable(distance));

                                        Point point = geometryFactory.createPoint(new Coordinate(signalLon, signalLat));
                                        point.setUserData(properties);
                                        result.add(new Tuple2<>(true, point));
                                        added = true;
                                    }
                                }
                            }
                        }

                        if (!added) {
                            result.add(new Tuple2<>(false, signal));
                        }
                    }

                    return result.iterator();
                });

        if (outputEvictedName != null) {
            Map<String, JavaRDDLike> ret = new HashMap<>();
            ret.put(outputSignalsName, signals.filter(t -> t._1).values());
            ret.put(outputEvictedName, signals.filter(t -> !t._1).values());

            return Collections.unmodifiableMap(ret);
        } else {
            return Collections.singletonMap(outputSignalsName, signals.filter(t -> t._1).values());
        }
    }
}
