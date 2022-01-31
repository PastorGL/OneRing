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
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.util.*;

import static ash.nazg.proximity.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class AreaCoversOperation extends Operation {
    private String inputGeometriesName;
    private String inputSignalsName;

    private Boolean once;

    private String outputSignalsName;
    private String outputEvictedName;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("areaCovers", "Takes a Point RDD and Polygon RDD and generates a Point RDD consisting" +
                " of all points that are contained inside the polygons",

                new NamedStreamsMetaBuilder()
                        .ds(RDD_INPUT_SIGNALS, "Source Point RDD",
                                new StreamType[]{StreamType.Point}, false
                        )
                        .ds(RDD_INPUT_GEOMETRIES, "Source Polygon RDD",
                                new StreamType[]{StreamType.Polygon}, false
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_ENCOUNTER_ONCE, "This flag suppresses creation of copies of a signal for each proximal geometry. If set to 'true'," +
                                        " properties of the source signal will be unchanged. Otherwise a copy of source signal will be created" +
                                        " for each proximal geometry, and their properties will be augmented with properties of that geometry",
                                Boolean.class, "false", "By default, create a distinct copy of a signal for each area it encounters inside")
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
        inputGeometriesName = opResolver.namedInput(RDD_INPUT_GEOMETRIES);
        inputSignalsName = opResolver.namedInput(RDD_INPUT_SIGNALS);

        once = opResolver.definition(OP_ENCOUNTER_ONCE);

        outputSignalsName = opResolver.namedOutput(RDD_OUTPUT_SIGNALS);
        outputEvictedName = opResolver.namedOutput(RDD_OUTPUT_EVICTED);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        String _inputGeometriesName = inputGeometriesName;
        String _inputSignalsName = inputSignalsName;
        boolean _once = once;

        JavaRDD<Polygon> geometriesInput = (JavaRDD<Polygon>) input.get(inputGeometriesName);

        final double maxRadius = geometriesInput
                .mapToDouble(poly -> {
                    Envelope ei = poly.getEnvelopeInternal();

                    return Geodesic.WGS84.Inverse(
                            (ei.getMaxY() + ei.getMinY()) / 2, (ei.getMaxX() + ei.getMinX()) / 2,
                            ei.getMaxY(), ei.getMaxX(), GeodesicMask.DISTANCE
                    ).s12;
                })
                .max(Comparator.naturalOrder());

        final SpatialUtils spatialUtils = new SpatialUtils(maxRadius);

        JavaPairRDD<Long, Polygon> hashedGeometries = geometriesInput
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Long, Polygon>> result = new ArrayList<>();

                    Text latAttr = new Text("_center_lat");
                    Text lonAttr = new Text("_center_lon");

                    while (it.hasNext()) {
                        Polygon o = it.next();

                        MapWritable properties = (MapWritable) o.getUserData();

                        result.add(new Tuple2<>(
                                spatialUtils.getHash(((DoubleWritable) properties.get(latAttr)).get(), ((DoubleWritable) properties.get(lonAttr)).get()), o)
                        );
                    }

                    return result.iterator();
                });

        JavaRDD<Point> inputSignals = (JavaRDD<Point>) input.get(inputSignalsName);

        Map<Long, Iterable<Polygon>> hashedGeometriesMap = hashedGeometries
                .groupByKey()
                .collectAsMap();

        // Broadcast hashed polys
        Broadcast<HashMap<Long, Iterable<Polygon>>> broadcastHashedGeometries = ctx
                .broadcast(new HashMap<>(hashedGeometriesMap));

        final GeometryFactory geometryFactory = new GeometryFactory();

        // Filter signals by hash coverage
        JavaPairRDD<Boolean, Point> signals = inputSignals
                .mapPartitionsToPair(it -> {
                    HashMap<Long, Iterable<Polygon>> geometries = broadcastHashedGeometries.getValue();

                    List<Tuple2<Boolean, Point>> result = new ArrayList<>();

                    while (it.hasNext()) {
                        Point signal = it.next();
                        boolean added = false;

                        MapWritable signalProperties = (MapWritable) signal.getUserData();

                        double signalLat = signal.getY();
                        double signalLon = signal.getX();
                        List<Long> neighood = spatialUtils.getNeighbours(signalLat, signalLon);

                        once:
                        for (Long hash : neighood) {
                            if (geometries.containsKey(hash)) {
                                for (Polygon geometry : geometries.get(hash)) {
                                    if (signal.within(geometry)) {
                                        if (_once) {
                                            result.add(new Tuple2<>(true, signal));
                                        } else {
                                            MapWritable properties = new MapWritable();
                                            ((MapWritable) geometry.getUserData()).forEach((k, v) -> properties.put(new Text(_inputGeometriesName + "." + k), new Text(String.valueOf(v))));
                                            properties.putAll(signalProperties);

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
