/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
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
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Polygon},
                                true
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.inputs.get(0);
        delimiter = dataStreamsProps.inputDelimiter(inputName);

        outputName = describedProps.outputs.get(0);

        String prop = describedProps.defs.getTyped(DS_CSV_HASH_COLUMN);
        hashColumn = dataStreamsProps.inputColumns.get(inputName).get(prop);

        String[] inputColumnsRaw = dataStreamsProps.inputColumnsRaw.get(inputName);
        Map<String, Integer> inputCols = new HashMap<>();
        for (int i = 0; i < inputColumnsRaw.length; i++) {
            inputCols.put(inputColumnsRaw[i], i);
        }
        List<String> outs = Arrays.stream(dataStreamsProps.outputColumns.get(outputName))
                .map(c -> c.replaceFirst("^" + inputName + "\\.", ""))
                .collect(Collectors.toList());
        Map<Integer, String> outputCols = new HashMap<>();
        for (int i = 0; i < outs.size(); i++) {
            outputCols.put(i, outs.get(i));
        }
        outputColumns = new HashMap<>();
        for (int i = 0; i < outs.size(); i++) {
            outputColumns.put(outputCols.get(i), inputCols.get(outputCols.get(i)));
        }
    }

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
