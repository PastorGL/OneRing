/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import com.opencsv.CSVWriter;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.spatial.config.ConfigurationParameters.GEN_HASH;

@SuppressWarnings("unused")
public class H3CompactCoverageOperation extends Operation {
    public static final String VERB = "h3CompactCoverage";

    @Description("Default finest hash level")
    public static final Integer DEF_HASH_LEVEL_TO = 9;
    @Description("Default coarsest hash level")
    public static final Integer DEF_HASH_LEVEL_FROM = 1;
    @Description("Level of the hash of the finest coverage unit")
    public static final String OP_HASH_LEVEL_TO = "hash.level.to";
    @Description("Level of the hash of the coarsest coverage unit")
    public static final String OP_HASH_LEVEL_FROM = "hash.level.from";
    @Description("Column with a generated hash level")
    public static final String GEN_LEVEL = "_level";

    private String inputName;

    protected Integer minLevel;
    protected Integer maxLevel;

    private String outputName;
    private char outputDelimiter;
    private List<String> outputColumns;
    private int outputParts;

    @Override
    @Description("Takes a Polygon RDD (with polygons sized as of a country) and generates a compact H3 coverage Polygon RDD," +
            " placing _hash property to each generated polygon")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_HASH_LEVEL_TO, Integer.class, DEF_HASH_LEVEL_TO),
                        new TaskDescriptionLanguage.Definition(OP_HASH_LEVEL_FROM, Integer.class, DEF_HASH_LEVEL_FROM),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.Polygon},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.CSV},
                                new String[]{GEN_HASH, GEN_LEVEL}
                        )
                )
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);
        outputDelimiter = dsResolver.outputDelimiter(outputName);

        maxLevel = opResolver.definition(OP_HASH_LEVEL_TO);
        minLevel = opResolver.definition(OP_HASH_LEVEL_FROM);

        if ((maxLevel < 1) || (maxLevel > 15)) {
            throw new InvalidConfigValueException("Finest hash level must fall into interval '1'..'15' but is '" + maxLevel + "' in the operation '" + name + "'");
        }
        if ((minLevel < 0) || (minLevel > 14)) {
            throw new InvalidConfigValueException("Coarsest hash level must fall into interval '0'..'14' but is '" + minLevel + "' in the operation '" + name + "'");
        }
        if (maxLevel <= minLevel) {
            throw new InvalidConfigValueException("Coarsest hash level must be higher than finest in the operation '" + name + "'");
        }

        outputColumns = Arrays.stream(dsResolver.outputColumns(outputName))
                .map(c -> c.replaceFirst("^" + inputName + "\\.", ""))
                .collect(Collectors.toList());

        outputParts = dsResolver.outputParts(outputName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        String _inputGeometriesName = inputName;

        JavaPairRDD<Long, Polygon> hashedGeometries = ((JavaRDD<Polygon>) input.get(inputName))
                .mapToPair(p -> new Tuple2<>(new Random().nextLong(), p));

        final GeometryFactory geometryFactory = new GeometryFactory();
        final int _maxLevel = maxLevel;

        int partCount = hashedGeometries.getNumPartitions();

        for (int level = minLevel; level <= maxLevel; level++) {
            final int _level = level;

            hashedGeometries = hashedGeometries
                    .mapPartitionsToPair(it -> {
                        List<Tuple2<Long, Polygon>> result = new ArrayList<>();

                        H3Core h3 = H3Core.newInstance();
                        Text hashAttr = new Text("_hash");
                        Text levelAttr = new Text("_level");
                        Text parentAttr = new Text("_parent");

                        while (it.hasNext()) {
                            Tuple2<Long, Polygon> o = it.next();

                            Polygon p = o._2;
                            MapWritable properties = (MapWritable) p.getUserData();
                            Text parent = new Text(String.valueOf(o._1));

                            if (!properties.containsKey(hashAttr)) {
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

                                    if (_level != _maxLevel) {
                                        holes.add(hole);
                                    }

                                    for (Coordinate c : hole.getCoordinates()) {
                                        gcii.add(new GeoCoord(c.y, c.x));
                                    }
                                    gci.add(gcii);
                                }

                                Text levelVal = new Text(Integer.toString(_level));
                                Set<Long> polyfill = new HashSet<>(h3.polyfill(gco, gci, _level));
                                Set<Long> hashes = new HashSet<>();
                                for (long hash : polyfill) {
                                    List<GeoCoord> geo = h3.h3ToGeoBoundary(hash);
                                    geo.add(geo.get(0));

                                    List<Coordinate> cl = new ArrayList<>();
                                    geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                                    Polygon polygon = geometryFactory.createPolygon(cl.toArray(new Coordinate[0]));
                                    MapWritable userData = new MapWritable(properties);
                                    userData.put(hashAttr, new Text(Long.toHexString(hash)));
                                    userData.put(levelAttr, levelVal);
                                    userData.put(parentAttr, parent);
                                    polygon.setUserData(userData);

                                    if (_level == _maxLevel) {
                                        List<Long> neighood = h3.kRing(hash, 1);
                                        neighood.forEach(neighash -> {
                                            if (!hashes.contains(neighash)) {
                                                List<GeoCoord> ng = h3.h3ToGeoBoundary(neighash);
                                                ng.add(ng.get(0));

                                                List<Coordinate> cn = new ArrayList<>();
                                                ng.forEach(c -> cn.add(new Coordinate(c.lng, c.lat)));

                                                Polygon neighpoly = geometryFactory.createPolygon(cn.toArray(new Coordinate[0]));
                                                MapWritable neighud = new MapWritable(properties);
                                                neighud.put(hashAttr, new Text(Long.toHexString(neighash)));
                                                neighud.put(levelAttr, levelVal);
                                                neighud.put(parentAttr, parent);
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

                                if (_level != _maxLevel) {
                                    Polygon nextPoly = geometryFactory.createPolygon(shell, holes.toArray(new LinearRing[0]));
                                    MapWritable nextData = new MapWritable(properties);
                                    nextPoly.setUserData(nextData);
                                    result.add(new Tuple2<>(o._1, nextPoly));
                                }
                            } else {
                                result.add(o);
                            }
                        }

                        return result.iterator();
                    })
                    .partitionBy(new RandomPartitioner(partCount));
        }

        final List<String> _outputColumns = outputColumns;
        final char _outputDelimiter = outputDelimiter;

        JavaRDD<Text> output = hashedGeometries.values()
                .mapPartitions(it -> {
                    List<Text> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Polygon p = it.next();

                        MapWritable props = (MapWritable) p.getUserData();

                        String[] out = new String[_outputColumns.size()];

                        int i = 0;
                        for (String column : _outputColumns) {
                            out[i++] = props.get(new Text(column)).toString();
                        }

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(out, false);
                        writer.close();

                        ret.add(new Text(buffer.toString()));
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }

    private static class RandomPartitioner extends Partitioner {
        private final int partCount;
        private final Random random;

        public RandomPartitioner(int partCount) {
            this.partCount = partCount;
            this.random = new Random();
        }

        @Override
        public int numPartitions() {
            return partCount;
        }

        @Override
        public int getPartition(Object key) {
            return random.nextInt(partCount);
        }
    }
}
