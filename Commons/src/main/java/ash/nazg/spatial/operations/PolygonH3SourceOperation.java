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
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class PolygonH3SourceOperation extends Operation {
    public static final String VERB = "polygonH3Source";

    private String inputName;

    private String outputName;
    private Map<String, Integer> outputColumns;
    private int hashColumn;
    private char delimiter;

    @Override
    @Description("Take a csv with H3 hashes and produce a Polygon RDD")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_CSV_HASH_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.Polygon},
                                true
                        )
                )
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        delimiter = dsResolver.inputDelimiter(inputName);

        outputName = opResolver.positionalOutput(0);

        String prop = opResolver.definition(DS_CSV_HASH_COLUMN);
        hashColumn = dsResolver.inputColumns(inputName).get(prop);

        String[] inputColumnsRaw = dsResolver.rawInputColumns(inputName);
        Map<String, Integer> inputCols = new HashMap<>();
        for (int i = 0; i < inputColumnsRaw.length; i++) {
            inputCols.put(inputColumnsRaw[i], i);
        }
        String[] outputCols = dsResolver.outputColumns(outputName);
        List<String> outs = (outputCols == null) ? Collections.emptyList() : Arrays.stream(outputCols)
                .map(c -> c.replaceFirst("^" + inputName + "\\.", ""))
                .collect(Collectors.toList());
        Map<Integer, String> outputMap = new HashMap<>();
        for (int i = 0; i < outs.size(); i++) {
            outputMap.put(i, outs.get(i));
        }
        outputColumns = new HashMap<>();
        for (int i = 0; i < outs.size(); i++) {
            outputColumns.put(outputMap.get(i), inputCols.get(outputMap.get(i)));
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Object> rdd = (JavaRDD<Object>) input.get(inputName);

        final char _delimiter = delimiter;
        final Map<String, Integer> _outputColumns = outputColumns;
        final GeometryFactory geometryFactory = new GeometryFactory();
        final int _hashColumn = hashColumn;

        JavaRDD<Polygon> output = rdd.mapPartitions(it -> {
            List<Polygon> ret = new ArrayList<>();

            CSVParser parser = new CSVParserBuilder().withSeparator(_delimiter).build();
            H3Core h3 = H3Core.newInstance();
            Text latAttr = new Text(GEN_CENTER_LAT);
            Text lonAttr = new Text(GEN_CENTER_LON);

            while (it.hasNext()) {
                String s = String.valueOf(it.next());

                MapWritable props = new MapWritable();
                String[] _columns = parser.parseLine(s);
                for (Map.Entry<String, Integer> e : _outputColumns.entrySet()) {
                    props.put(new Text(e.getKey()), new Text(_columns[e.getValue()]));
                }

                long hash = Long.parseUnsignedLong(_columns[_hashColumn], 16);
                List<GeoCoord> geo = h3.h3ToGeoBoundary(hash);
                geo.add(geo.get(0));

                List<Coordinate> cl = new ArrayList<>();
                geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                Polygon polygon = geometryFactory.createPolygon(cl.toArray(new Coordinate[0]));
                Point centroid = polygon.getCentroid();
                props.put(latAttr, new DoubleWritable(centroid.getY()));
                props.put(lonAttr, new DoubleWritable(centroid.getX()));
                polygon.setUserData(props);

                ret.add(polygon);
            }

            return ret.iterator();
        });

        return Collections.singletonMap(outputName, output);
    }
}
