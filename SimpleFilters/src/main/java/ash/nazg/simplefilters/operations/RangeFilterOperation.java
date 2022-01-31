/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class RangeFilterOperation extends Operation {
    public static final String OP_FILTERING_RANGE = "filtering.range";
    public static final String DS_FILTERING_COLUMN = "filtering.column";

    private String inputName;
    private char inputDelimiter;
    private Integer filteringColumn;

    private String outputName;

    private Tuple2<Double, Double> range;
    private Tuple2<Boolean, Boolean> inclusive;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("rangeFilter", "In a CSV RDD, take a column to filter all rows that have" +
                " a Double value in this column that lies outside of the set absolute range",

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_FILTERING_COLUMN, "Column with Double values to apply the filter")
                        .def(OP_FILTERING_RANGE, "Range syntax is [BOTTOM;TOP) where brackets mean inclusive" +
                                " border and parens exclusive. Either boundary is optional, but not both at the same time." +
                                " Examples: (0 1000], []-7.47;7.48, [-1000;)")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Filtered CSV RDD",
                                new StreamType[]{StreamType.Passthru}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputName = opResolver.positionalOutput(0);

        String prop;

        prop = opResolver.definition(OP_FILTERING_RANGE);
        inclusive = new Tuple2<>(
                prop.contains("["),
                prop.contains("]")
        );
        String[] bounds = prop
                .trim()
                .replaceAll("[\\[\\]()]", "")
                .split("[;\\s]+", 2);
        range = new Tuple2<>(
                bounds[0].isEmpty() ? null : Double.parseDouble(bounds[0]),
                bounds[1].isEmpty() ? null : Double.parseDouble(bounds[1])
        );

        if ((range._1 == null) && (range._2 == null)) {
            throw new InvalidConfigValueException("Setting '" + OP_FILTERING_RANGE + "' for an operation '" + name + "' must have at least one of upper and lower boundaries set");
        }

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);

        prop = opResolver.definition(DS_FILTERING_COLUMN);
        filteringColumn = inputColumns.get(prop);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter = inputDelimiter;
        final Integer _filteringColumn = filteringColumn;
        final Tuple2<Double, Double> _range = range;
        final Tuple2<Boolean, Boolean> _inclusive = inclusive;

        JavaRDD<Object> output = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(it -> {
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter)
                            .build();

                    List<Object> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Object o = it.next();
                        String l = o instanceof String ? (String) o : String.valueOf(o);
                        String[] row = parser.parseLine(l);
                        String strValue = row[_filteringColumn];

                        boolean include = true;
                        if ((strValue != null) && !strValue.isEmpty()) {
                            try {
                                Double value = Double.parseDouble(strValue);

                                if (_range._1 != null) {
                                    include &= _inclusive._1 ? _range._1 <= value : _range._1 < value;
                                }
                                if (_range._2 != null) {
                                    include &= _inclusive._2 ? value <= _range._2 : value < _range._2;
                                }
                            } catch (NumberFormatException ignore) {
                                // include non-number
                            }
                        }

                        if (include) {
                            ret.add(o);
                        }
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
