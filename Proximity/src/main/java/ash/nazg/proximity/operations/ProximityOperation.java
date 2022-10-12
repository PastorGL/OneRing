/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.proximity.operations;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.data.DataStream;
import ash.nazg.data.StreamType;
import ash.nazg.data.spatial.PointEx;
import ash.nazg.metadata.*;
import ash.nazg.scripting.Operation;
import ash.nazg.spatial.SpatialUtils;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicMask;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.util.*;

@SuppressWarnings("unused")
public class ProximityOperation extends Operation {
    static final String ENCOUNTER_MODE = "encounter.mode";
    static final String INPUT_POINTS = "points";
    static final String INPUT_POIS = "pois";
    static final String OUTPUT_TARGET = "target";
    static final String OUTPUT_EVICTED = "evicted";
    private EncounterMode once;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("proximity", "Takes a Point DataStream and POI DataStream and generates a Point DataStream consisting" +
                " of all Points that are within the range of POIs (in different encounter modes)",

                new NamedStreamsMetaBuilder()
                        .mandatoryInput(INPUT_POINTS, "Source Point DataStream",
                                new StreamType[]{StreamType.Point}
                        )
                        .mandatoryInput(INPUT_POIS, "Source POI DataStream with vicinity radius property set",
                                new StreamType[]{StreamType.Point}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(ENCOUNTER_MODE, "How to treat signal a target one in regard of multiple POIs in the vicinity",
                                EncounterMode.class, EncounterMode.COPY, "By default, create a distinct copy of a signal for each POI" +
                                        " it encounters in the proximity radius")
                        .build(),

                new NamedStreamsMetaBuilder()
                        .mandatoryOutput(OUTPUT_TARGET, "Output Point DataStream with target signals",
                                new StreamType[]{StreamType.Point}, Origin.AUGMENTED, Arrays.asList(INPUT_POINTS, INPUT_POIS)
                        )
                        .generated(OUTPUT_TARGET, "_distance", "Distance from POI for " + ENCOUNTER_MODE + "=" + EncounterMode.COPY.name())
                        .optionalOutput(OUTPUT_EVICTED, "Optional output Point DataStream with evicted signals",
                                new StreamType[]{StreamType.Point}, Origin.FILTERED, Collections.singletonList(INPUT_POINTS)
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        once = params.get(ENCOUNTER_MODE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, DataStream> execute() {
        EncounterMode _once = once;

        DataStream inputSignals = inputStreams.get(INPUT_POINTS);
        DataStream inputPois = inputStreams.get(INPUT_POIS);

        // Get POIs radii
        JavaRDD<Tuple2<Double, PointEx>> poiRadii = ((JavaRDD<PointEx>) inputPois.get())
                .mapPartitions(it -> {
                    List<Tuple2<Double, PointEx>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        PointEx o = it.next();

                        double radius = o.getRadius();
                        result.add(new Tuple2<>(radius, o));
                    }

                    return result.iterator();
                });

        final double maxRadius = poiRadii
                .map(t -> t._1)
                .max(Comparator.naturalOrder());

        final SpatialUtils spatialUtils = new SpatialUtils(maxRadius);

        // hash -> radius, poi
        JavaPairRDD<Long, Tuple2<Double, PointEx>> hashedPois = poiRadii
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Long, Tuple2<Double, PointEx>>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Tuple2<Double, PointEx> o = it.next();

                        result.add(new Tuple2<>(
                                spatialUtils.getHash(o._2.getY(), o._2.getX()),
                                new Tuple2<>(o._1, o._2))
                        );
                    }

                    return result.iterator();
                });

        Map<Long, Iterable<Tuple2<Double, PointEx>>> hashedPoisMap = hashedPois
                .groupByKey()
                .collectAsMap();

        JavaRDD<PointEx> signalsInput = (JavaRDD<PointEx>) inputSignals.get();

        // Broadcast hashed POIs
        Broadcast<HashMap<Long, Iterable<Tuple2<Double, PointEx>>>> broadcastHashedPois = JavaSparkContext.fromSparkContext(signalsInput.context())
                .broadcast(new HashMap<>(hashedPoisMap));

        final GeometryFactory geometryFactory = new GeometryFactory();
        final CoordinateSequenceFactory csFactory = geometryFactory.getCoordinateSequenceFactory();
        final int poiCount = hashedPoisMap.size();

        // Filter signals by hash coverage
        JavaPairRDD<Boolean, PointEx> signals = signalsInput
                .mapPartitionsToPair(it -> {
                    HashMap<Long, Iterable<Tuple2<Double, PointEx>>> pois = broadcastHashedPois.getValue();

                    List<Tuple2<Boolean, PointEx>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        PointEx signal = it.next();
                        boolean target = false;

                        double signalLat = signal.getY();
                        double signalLon = signal.getX();
                        List<Long> neighood = spatialUtils.getNeighbours(signalLat, signalLon);
                        int near = 0;

                        once:
                        for (Long hash : neighood) {
                            if (pois.containsKey(hash)) {
                                for (Tuple2<Double, PointEx> poi : pois.get(hash)) {
                                    double distance = Geodesic.WGS84.Inverse(signalLat, signalLon, poi._2.getY(), poi._2.getX(), GeodesicMask.DISTANCE).s12;

                                    //check if poi falls into radius
                                    switch (_once) {
                                        case ONCE: {
                                            if (distance <= poi._1) {
                                                result.add(new Tuple2<>(true, signal));
                                                target = true;
                                                break once;
                                            }
                                            break;
                                        }
                                        case COPY: {
                                            if (distance <= poi._1) {
                                                PointEx point = new PointEx(signal);
                                                point.put((Map) signal.getUserData());
                                                point.put((Map) poi._2.getUserData());
                                                point.put("_distance", distance);
                                                result.add(new Tuple2<>(true, point));
                                                target = true;
                                            }
                                            break;
                                        }
                                        case ALL: {
                                            if (distance > poi._1) {
                                                break once;
                                            } else {
                                                target = true;
                                                near++;
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        if ((_once == EncounterMode.ALL) && (near == poiCount)) {
                            result.add(new Tuple2<>(true, signal));
                        } else {
                            target = false;
                        }
                        if (!target) {
                            result.add(new Tuple2<>(false, signal));
                        }
                    }

                    return result.iterator();
                });

        Map<String, DataStream> ret = new HashMap<>();
        List<String> outputColumns = new ArrayList<>(inputSignals.accessor.attributes("point"));
        if (once == EncounterMode.COPY) {
            outputColumns.addAll(inputPois.accessor.attributes("point"));
            outputColumns.add("_distance");
        }
        ret.put(outputStreams.get(OUTPUT_TARGET), new DataStream(StreamType.Point, signals.filter(t -> t._1).values(), Collections.singletonMap("point", outputColumns)));

        String outputEvictedName = outputStreams.get(OUTPUT_EVICTED);
        if (outputEvictedName != null) {
            ret.put(outputEvictedName, new DataStream(StreamType.Point, signals.filter(t -> !t._1).values(), Collections.singletonMap("point", inputSignals.accessor.attributes("point"))));
        }

        return Collections.unmodifiableMap(ret);
    }

    private enum EncounterMode implements DefinitionEnum {
        ONCE("This flag suppresses creation of copies of a signal for each proximal POI." +
                " Properties of the source signal will be unchanged"),
        COPY("For this flag, a distinct copy of source signal will be created for each proximal POI," +
                " and their properties will be augmented with properties of that POI"),
        ALL("This flag emits only signals that in the intersection of all input POI vicinities with unchanged properties." +
                " If POI vicinity radii don't intersect, no signals will be emitted");

        private final String descr;

        EncounterMode(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
