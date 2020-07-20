/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.proximity.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
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
public class LandScaleOperation extends Operation {
    public static final String VERB = "landCovers";

    @Description("Default hash level")
    public static final Integer DEF_HASH_LEVEL = 9;
    @Description("Level of the hash of the finest coverage unit")
    public static final String OP_HASH_LEVEL = "hash.level";

    private String inputGeometriesName;
    private String inputSignalsName;

    protected Integer level;

    private String outputSignalsName;

    @Override
    @Description("Takes a Point RDD and a Polygon RDD (with polygons sized as of a country) and generates a coarse coverage Point RDD")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_HASH_LEVEL, Integer.class, DEF_HASH_LEVEL),
                },

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
                                )
                        }
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputGeometriesName = describedProps.namedInputs.get(RDD_INPUT_GEOMETRIES);
        inputSignalsName = describedProps.namedInputs.get(RDD_INPUT_SIGNALS);

        outputSignalsName = describedProps.namedOutputs.get(RDD_OUTPUT_SIGNALS);

        level = describedProps.defs.getTyped(OP_HASH_LEVEL);

        if ((level < 3) || (level > 15)) {
            throw new InvalidConfigValueException("Hash level must fall into interval '3'..'15' but is '" + level + "' in the operation '" + name + "'");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        String _inputGeometriesName = inputGeometriesName;
        String _inputSignalsName = inputSignalsName;

        JavaRDD<Polygon> geometriesInput = (JavaRDD<Polygon>) input.get(inputGeometriesName);

        final GeometryFactory geometryFactory = new GeometryFactory();
        final int _maxLevel = level;

        JavaPairRDD<Long, Polygon> hashedGeometries = geometriesInput.mapToPair(p -> new Tuple2<>(null, p));
        for (int lev = 3; lev <= level; lev++) {
            final int _level = lev;

            hashedGeometries = hashedGeometries
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Long, Polygon>> result = new ArrayList<>();

                        H3Core h3 = H3Core.newInstance();

                        while (it.hasNext()) {
                            Tuple2<Long, Polygon> o = it.next();

                            if (o._1 == null) {
                                Polygon p = o._2;

                                MapWritable properties = (MapWritable) p.getUserData();

                                List<GeoCoord> gco = new ArrayList<>();
                                LinearRing shell = (LinearRing) p.getExteriorRing();
                                for (Coordinate c : shell.getCoordinates()) {
                                    gco.add(new GeoCoord(c.y, c.x));
                                }

                                List<LinearRing> holes = new ArrayList<>();

                                List<List<GeoCoord>> gci = new ArrayList<>();
                                for (int i = p.getNumInteriorRing(); i > 0; ) {
                                    List<GeoCoord> gcii = new ArrayList<>();
                                    LinearRing hole = (LinearRing) p.getInteriorRingN(--i);

                                    holes.add(hole);

                                    for (Coordinate c : hole.getCoordinates()) {
                                        gcii.add(new GeoCoord(c.y, c.x));
                                    }
                                    gci.add(gcii);
                                }

                                List<Long> inters = h3.polyfill(gco, gci, _level);
                                for (long hash : inters) {
                                    List<GeoCoord> geo = h3.h3ToGeoBoundary(hash);
                                    geo.add(geo.get(0));

                                    List<Coordinate> cl = new ArrayList<>();
                                    geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                                    Polygon polygon = geometryFactory.createPolygon(cl.toArray(new Coordinate[0]));
                                    polygon.setUserData(new MapWritable(properties));

                                    result.add(new Tuple2<>(hash, polygon));

                                    Collections.reverse(cl);
                                    LinearRing hole = geometryFactory.createLinearRing(cl.toArray(new Coordinate[0]));
                                    holes.add(hole);
                                }

                                if (_level < _maxLevel) {
                                    Polygon polygon = geometryFactory.createPolygon(shell, holes.toArray(new LinearRing[0]));
                                    result.add(new Tuple2<>(null, polygon));
                                }
                            } else {
                                result.add(o);
                            }
                        }

                        return result.iterator();
                    });
        }

        JavaRDD<Point> inputSignals = (JavaRDD<Point>) input.get(inputSignalsName);

        Map<Long, Iterable<Polygon>> hashedGeometriesMap = hashedGeometries
                .groupByKey()
                .collectAsMap();

        // Broadcast hashed centroids
        Broadcast<HashMap<Long, Iterable<Polygon>>> broadcastHashedGeometries = ctx
                .broadcast(new HashMap<>(hashedGeometriesMap));

        final Set<Long> geometryHashes = new HashSet<>(hashedGeometriesMap.keySet());

        // Filter signals by hash coverage
        JavaRDD<Point> signals = inputSignals
                .mapPartitions(it -> {
                    HashMap<Long, Iterable<Polygon>> geometries = broadcastHashedGeometries.getValue();

                    List<Point> result = new ArrayList<>();
                    H3Core h3 = H3Core.newInstance();

                    while (it.hasNext()) {
                        Point signal = it.next();

                        MapWritable signalProperties = (MapWritable) signal.getUserData();

                        long signalHash = h3.geoToH3(signal.getY(), signal.getX(), _maxLevel);

                        List<Long> hashes = new ArrayList<>();
                        for (Long g : geometryHashes) {
                            if (g == signalHash || g == h3.h3ToParent(signalHash, h3.h3GetResolution(g))) {
                                hashes.add(g);
                            }
                        }

                        for (long hash : hashes) {
                            for (Polygon geometry : geometries.get(hash)) {
                                MapWritable properties = new MapWritable();
                                ((MapWritable) geometry.getUserData()).forEach((k, v) -> properties.put(new Text(_inputGeometriesName + "." + k), new Text(String.valueOf(v))));
                                properties.putAll(signalProperties);

                                Point point = geometryFactory.createPoint(new Coordinate(signal.getY(), signal.getX()));
                                point.setUserData(properties);
                                result.add(point);
                            }
                        }
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputSignalsName, signals);
    }
}
