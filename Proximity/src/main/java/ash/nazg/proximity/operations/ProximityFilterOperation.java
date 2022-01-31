/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.proximity.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.NamedStreamsMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
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

import static ash.nazg.proximity.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class ProximityFilterOperation extends Operation {
    private String inputSignalsName;
    private String inputPoisName;

    private Boolean once;

    private String outputSignalsName;
    private String outputEvictedName;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("proximityFilter", "Takes a Point RDD and POI Point RDD and generates a Point RDD consisting" +
                " of all points that are within the range of POIs",

                new NamedStreamsMetaBuilder()
                        .ds(RDD_INPUT_SIGNALS, "Source Point RDD",
                                new StreamType[]{StreamType.Point}, false
                        )
                        .ds(RDD_INPUT_POIS, "Source POI Point RDD with _radius attribute set",
                                new StreamType[]{StreamType.Point}, false
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_ENCOUNTER_ONCE, "This flag suppresses creation of copies of a signal for each proximal geometry. If set to 'true'," +
                                        " properties of the source signal will be unchanged. Otherwise a copy of source signal will be created" +
                                        " for each proximal geometry, and their properties will be augmented with properties of that geometry",
                                Boolean.class, "false", "By default, create a distinct copy of a signal for each POI it encounters in the proximity radius")
                        .build(),

                new NamedStreamsMetaBuilder()
                        .ds(RDD_OUTPUT_SIGNALS, "Output Point RDD with target signals",
                                new StreamType[]{StreamType.Point}, false
                        )
                        .ds(RDD_OUTPUT_EVICTED, "Optional output Point RDD with evicted signals",
                                new StreamType[]{StreamType.Point}, false
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputSignalsName = opResolver.namedInput(RDD_INPUT_SIGNALS);
        inputPoisName = opResolver.namedInput(RDD_INPUT_POIS);

        once = opResolver.definition(OP_ENCOUNTER_ONCE);

        outputSignalsName = opResolver.namedOutput(RDD_OUTPUT_SIGNALS);
        outputEvictedName = opResolver.namedOutput(RDD_OUTPUT_EVICTED);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final String _inputSignalsName = inputSignalsName;
        final String _inputPoisName = inputPoisName;
        boolean _once = once;

        JavaRDD<Point> inputSignals = (JavaRDD<Point>) input.get(inputSignalsName);

        JavaRDD<Point> inputPois = (JavaRDD<Point>) input.get(inputPoisName);

        // Get POIs radii
        JavaRDD<Tuple2<Double, Point>> poiRadii = inputPois
                .mapPartitions(it -> {
                    List<Tuple2<Double, Point>> result = new ArrayList<>();

                    Text radiusAttr = new Text("_radius");

                    while (it.hasNext()) {
                        Point o = it.next();

                        MapWritable properties = (MapWritable) o.getUserData();

                        double radius = ((DoubleWritable) properties.get(radiusAttr)).get();
                        result.add(new Tuple2<>(radius, o));
                    }

                    return result.iterator();
                });

        final double maxRadius = poiRadii
                .map(t -> t._1)
                .max(Comparator.naturalOrder());

        final SpatialUtils spatialUtils = new SpatialUtils(maxRadius);

        // hash -> radius, poi
        JavaPairRDD<Long, Tuple2<Double, Point>> hashedPois = poiRadii
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Long, Tuple2<Double, Point>>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Double, Point> o = it.next();

                        result.add(new Tuple2<>(
                                spatialUtils.getHash(o._2.getY(), o._2.getX()),
                                new Tuple2<>(o._1, o._2))
                        );
                    }

                    return result.iterator();
                });

        Map<Long, Iterable<Tuple2<Double, Point>>> hashedPoisMap = hashedPois
                .groupByKey()
                .collectAsMap();

        // Broadcast hashed POIs
        Broadcast<HashMap<Long, Iterable<Tuple2<Double, Point>>>> broadcastHashedPois = ctx
                .broadcast(new HashMap<>(hashedPoisMap));

        final GeometryFactory geometryFactory = new GeometryFactory();

        // Filter signals by hash coverage
        JavaPairRDD<Boolean, Point> signals = inputSignals
                .mapPartitionsToPair(it -> {
                    HashMap<Long, Iterable<Tuple2<Double, Point>>> pois = broadcastHashedPois.getValue();

                    List<Tuple2<Boolean, Point>> result = new ArrayList<>();

                    Text distanceAttr = new Text("_distance");

                    while (it.hasNext()) {
                        Point signal = it.next();
                        boolean added = false;

                        MapWritable signalProperties = (MapWritable) signal.getUserData();

                        double signalLat = signal.getY();
                        double signalLon = signal.getX();
                        List<Long> neighood = spatialUtils.getNeighbours(signalLat, signalLon);

                        once:
                        for (Long hash : neighood) {
                            if (pois.containsKey(hash)) {
                                for (Tuple2<Double, Point> poi : pois.get(hash)) {
                                    double distance = Geodesic.WGS84.Inverse(signalLat, signalLon, poi._2.getY(), poi._2.getX(), GeodesicMask.DISTANCE).s12;

                                    //check if poi falls into radius
                                    if (distance <= poi._1) {
                                        if (_once) {
                                            result.add(new Tuple2<>(true, signal));
                                        } else {
                                            MapWritable poiProperties = (MapWritable) poi._2.getUserData();
                                            MapWritable properties = new MapWritable();
                                            poiProperties.forEach((k, v) -> properties.put(new Text(_inputPoisName + "." + k), new Text(String.valueOf(v))));
                                            properties.putAll(signalProperties);
                                            properties.put(distanceAttr, new DoubleWritable(distance));

                                            Point point = geometryFactory.createPoint(new Coordinate(signalLon, signalLat));
                                            point.setUserData(properties);
                                            result.add(new Tuple2<>(true, point));
                                        }
                                        added = true;
                                    }

                                    if (_once && added) {
                                        break once;
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
