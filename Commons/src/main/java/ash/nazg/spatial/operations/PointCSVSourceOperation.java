/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.*;
import java.util.stream.Collectors;

import static ash.nazg.spatial.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class PointCSVSourceOperation extends Operation {
    private String inputName;
    private char inputDelimiter;
    private int latColumn;
    private int lonColumn;
    private Integer radiusColumn;

    private String outputName;
    private Map<String, Integer> outputColumns;

    private Double defaultRadius;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("pointCsvSource", "Take a CSV file and produce a Polygon RDD",

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD with Point attributes",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_DEFAULT_RADIUS, "If set, generated Points will have this value in the _radius parameter",
                                Double.class, null, "By default, don't set Point _radius attribute")
                        .def(DS_CSV_RADIUS_COLUMN, "If set, generated Points will take their _radius parameter from the specified column instead",
                                null, "By default, don't set Point _radius attribute")
                        .def(DS_CSV_LAT_COLUMN, "Point latitude column")
                        .def(DS_CSV_LON_COLUMN, "Point longitude column")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Point RDD generated from CSV input",
                                new StreamType[]{StreamType.Point}, true
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);

        inputDelimiter = dsResolver.inputDelimiter(inputName);

        defaultRadius = opResolver.definition(OP_DEFAULT_RADIUS);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);
        String[] outputCols = dsResolver.outputColumns(outputName);
        final List<String> outColumns = (outputCols == null) ? Collections.emptyList() : Arrays.asList(outputCols);
        outputColumns = inputColumns.entrySet().stream()
                .filter(c -> outColumns.isEmpty() || outColumns.contains(c.getKey()))
                .collect(Collectors.toMap(c -> c.getKey().replaceFirst("^[^.]+\\.", ""), Map.Entry::getValue));

        String prop;

        prop = opResolver.definition(DS_CSV_RADIUS_COLUMN);
        radiusColumn = inputColumns.get(prop);

        prop = opResolver.definition(DS_CSV_LAT_COLUMN);
        latColumn = inputColumns.get(prop);

        prop = opResolver.definition(DS_CSV_LON_COLUMN);
        lonColumn = inputColumns.get(prop);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final int _latColumn = latColumn;
        final int _lonColumn = lonColumn;
        final Integer _radiusColumn = radiusColumn;
        final Double _defaultRadius = defaultRadius;
        final char _inputDelimiter = inputDelimiter;
        final Map<String, Integer> _outputColumns = outputColumns;
        final GeometryFactory geometryFactory = new GeometryFactory();

        JavaRDD<Point> output = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(it -> {
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

                    List<Point> result = new ArrayList<>();

                    Text latAttr = new Text(GEN_CENTER_LAT);
                    Text lonAttr = new Text(GEN_CENTER_LON);
                    Text radiusAttr = new Text(GEN_RADIUS);

                    while (it.hasNext()) {
                        Object line = it.next();
                        String l = line instanceof String ? (String) line : String.valueOf(line);

                        String[] row = parser.parseLine(l);

                        double lat = Double.parseDouble(row[_latColumn]);
                        double lon = Double.parseDouble(row[_lonColumn]);

                        MapWritable properties = new MapWritable();

                        for (Map.Entry<String, Integer> col : _outputColumns.entrySet()) {
                            properties.put(new Text(col.getKey()), new Text(row[col.getValue()]));
                        }

                        Double radius = _defaultRadius;
                        if (_radiusColumn != null) {
                            radius = Double.parseDouble(row[_radiusColumn]);
                        }

                        Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
                        point.setUserData(properties);
                        properties.put(latAttr, new DoubleWritable(lat));
                        properties.put(lonAttr, new DoubleWritable(lon));
                        if (radius != null) {
                            properties.put(radiusAttr, new DoubleWritable(radius));
                        }

                        result.add(point);
                    }

                    return result.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
