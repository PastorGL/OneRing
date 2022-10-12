/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.transform;

import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import ash.nazg.metadata.TransformedStreamMetaBuilder;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.util.*;

@SuppressWarnings("unused")
public class PolygonToH3CompactCoverage implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("h3CompactCoverage", StreamType.Polygon, StreamType.Columnar,
                "Takes a Polygon DataStream (with polygons sized as of a country) and generates a Columnar one with compact H3 coverage for each Polygon",

                new DefinitionMetaBuilder()
                        .def("hash.level.to", "Level of the hash of the finest coverage unit",
                                Integer.class, 9, "Default finest hash level")
                        .def("hash.level.from", "Level of the hash of the coarsest coverage unit",
                                Integer.class, 1, "Default coarsest hash level")
                        .build(),
                new TransformedStreamMetaBuilder()
                        .genCol("_hash", "Polygon H3 hash")
                        .genCol("_level", "H3 hash level")
                        .genCol("_parent", "Parent Polygon H3 hash")
                        .build()
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            List<String> valueColumns = newColumns.get("value");
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes("polygon");
            }

            final List<String> _outputColumns = valueColumns;

            final Integer levelTo = params.get("hash.level.to");
            final Integer levelFrom = params.get("hash.level.from");

            JavaPairRDD<Long, Polygon> hashedGeometries = ((JavaRDD<Polygon>) ds.get())
                    .mapToPair(p -> new Tuple2<>(new Random().nextLong(), p));

            final GeometryFactory geometryFactory = new GeometryFactory();

            final int partCount = hashedGeometries.getNumPartitions();

            for (int lvl = levelFrom; lvl <= levelTo; lvl++) {
                final int _level = lvl;

                hashedGeometries = hashedGeometries
                        .mapPartitionsToPair(it -> {
                            List<Tuple2<Long, Polygon>> result = new ArrayList<>();

                            H3Core h3 = H3Core.newInstance();

                            while (it.hasNext()) {
                                Tuple2<Long, Polygon> o = it.next();

                                Polygon p = o._2;
                                Map<String, Object> properties = (Map<String, Object>) p.getUserData();
                                Long parent = o._1;

                                if (!properties.containsKey("_hash")) {
                                    List<GeoCoord> gco = new ArrayList<>();
                                    LinearRing shell = p.getExteriorRing();
                                    for (Coordinate c : shell.getCoordinates()) {
                                        gco.add(new GeoCoord(c.y, c.x));
                                    }

                                    List<LinearRing> holes = new ArrayList<>();

                                    List<List<GeoCoord>> gci = new ArrayList<>();
                                    for (int i = p.getNumInteriorRing(); i > 0; ) {
                                        List<GeoCoord> gcii = new ArrayList<>();
                                        LinearRing hole = p.getInteriorRingN(--i);

                                        if (_level != levelTo) {
                                            holes.add(hole);
                                        }

                                        for (Coordinate c : hole.getCoordinates()) {
                                            gcii.add(new GeoCoord(c.y, c.x));
                                        }
                                        gci.add(gcii);
                                    }

                                    Set<Long> polyfill = new HashSet<>(h3.polyfill(gco, gci, _level));
                                    Set<Long> hashes = new HashSet<>();
                                    for (long hash : polyfill) {
                                        List<GeoCoord> geo = h3.h3ToGeoBoundary(hash);
                                        geo.add(geo.get(0));

                                        List<Coordinate> cl = new ArrayList<>();
                                        geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                                        Polygon polygon = geometryFactory.createPolygon(cl.toArray(new Coordinate[0]));
                                        Map<String, Object> userData = new HashMap<>(properties);
                                        userData.put("_hash", Long.toHexString(hash));
                                        userData.put("_level", _level);
                                        userData.put("_parent", parent);
                                        polygon.setUserData(userData);

                                        if (_level == levelTo) {
                                            List<Long> neighood = h3.kRing(hash, 1);
                                            neighood.forEach(neighash -> {
                                                if (!hashes.contains(neighash)) {
                                                    List<GeoCoord> ng = h3.h3ToGeoBoundary(neighash);
                                                    ng.add(ng.get(0));

                                                    List<Coordinate> cn = new ArrayList<>();
                                                    ng.forEach(c -> cn.add(new Coordinate(c.lng, c.lat)));

                                                    Polygon neighpoly = geometryFactory.createPolygon(cn.toArray(new Coordinate[0]));
                                                    Map<String, Object> neighud = new HashMap<>(properties);
                                                    neighud.put("_hash", Long.toHexString(neighash));
                                                    neighud.put("_level", _level);
                                                    neighud.put("_parent", parent);
                                                    neighpoly.setUserData(neighud);

                                                    result.add(new Tuple2<>(o._1, neighpoly));
                                                    hashes.add(neighash);
                                                }
                                            });

                                            if (!hashes.contains(hash)) {
                                                result.add(new Tuple2<>(o._1, polygon));
                                                hashes.add(hash);
                                            }
                                        } else {
                                            if (polyfill.containsAll(h3.kRing(hash, 1))) {
                                                Collections.reverse(cl);
                                                LinearRing hole = geometryFactory.createLinearRing(cl.toArray(new Coordinate[0]));
                                                holes.add(hole);

                                                result.add(new Tuple2<>(o._1, polygon));
                                            }
                                        }
                                    }

                                    if (_level != levelTo) {
                                        Polygon nextPoly = geometryFactory.createPolygon(shell, holes.toArray(new LinearRing[0]));
                                        Map<String, Object> nextData = new HashMap<>(properties);
                                        nextPoly.setUserData(nextData);
                                        result.add(new Tuple2<>(o._1, nextPoly));
                                    }
                                } else {
                                    result.add(o);
                                }
                            }

                            return result.iterator();
                        })
                        .partitionBy(new Partitioner() {
                            private final Random random = new Random();

                            @Override
                            public int numPartitions() {
                                return partCount;
                            }

                            @Override
                            public int getPartition(Object key) {
                                return random.nextInt(partCount);
                            }
                        });
            }

            return new DataStream(StreamType.Columnar, hashedGeometries.values()
                    .mapPartitions(it -> {
                        List<Columnar> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Polygon p = it.next();

                            Map<String, Object> props = (Map<String, Object>) p.getUserData();

                            Columnar rec = new Columnar(_outputColumns);
                            rec.put(props);

                            ret.add(rec);
                        }

                        return ret.iterator();
                    }), newColumns);
        };
    }
}
