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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.Collections;
import java.util.Map;

@SuppressWarnings("unused")
public class PercentileFilterOperation extends Operation {
    public static final String DS_FILTERING_COLUMN = "filtering.column";
    public static final String OP_PERCENTILE_TOP = "percentile.top";
    public static final String OP_PERCENTILE_BOTTOM = "percentile.bottom";

    private String inputName;
    private char inputDelimiter;
    private Integer filteringColumn;

    private String outputName;

    private byte topPercentile;
    private byte bottomPercentile;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("percentileFilter", "In a CSV RDD, take a column to filter all rows that have" +
                " a Double value in this column that lies outside of the set percentile range",

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD",
                                new StreamType[]{StreamType.CSV},
                                true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_FILTERING_COLUMN, "Column with Double values to apply the filter")
                        .def(OP_PERCENTILE_BOTTOM, "Bottom of percentile range (inclusive)", Byte.class,
                                "-1", "By default, do not set bottom percentile")
                        .def(OP_PERCENTILE_TOP, "Top of percentile range (inclusive)", Byte.class,
                                "-1", "By default, do not set top percentile")
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

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);
        String prop;

        prop = opResolver.definition(DS_FILTERING_COLUMN);
        filteringColumn = inputColumns.get(prop);

        topPercentile = opResolver.definition(OP_PERCENTILE_TOP);
        if (topPercentile > 100) {
            topPercentile = 100;
        }

        bottomPercentile = opResolver.definition(OP_PERCENTILE_BOTTOM);
        if (bottomPercentile > 100) {
            bottomPercentile = 100;
        }

        if ((topPercentile < 0) && (bottomPercentile < 0)) {
            throw new InvalidConfigValueException("Check if '" + OP_PERCENTILE_TOP + "' and/or '" + OP_PERCENTILE_BOTTOM + "' for operation '" + name + "' are set");
        }

        if ((topPercentile >= 0) && (bottomPercentile >= 0) && (topPercentile < bottomPercentile)) {
            throw new InvalidConfigValueException("Check if value of '" + OP_PERCENTILE_TOP + "' is greater than value of '" + OP_PERCENTILE_BOTTOM + "' for operation '" + name + "'");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Object> inputRDD = (JavaRDD<Object>) input.get(inputName);

        char _inputDelimiter = inputDelimiter;
        int _filteringColumn = filteringColumn;

        JavaRDD<Tuple2<Double, String>> series = inputRDD
                .map(o -> {
                    String l = o instanceof String ? (String) o : String.valueOf(o);
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                    String[] row = parser.parseLine(l);

                    return new Tuple2<>(new Double(row[_filteringColumn]), l);
                });

        JavaPairRDD<Long, Tuple2<Double, String>> percentiles = series
                .sortBy(d -> d._1, true, inputRDD.getNumPartitions())
                .zipWithIndex()
                .mapToPair(Tuple2::swap);

        long count = series.count();

        double top = 0.D, bottom = 0.D;
        if (topPercentile >= 0) {
            top = percentiles.lookup((long) (count * topPercentile / 100.D)).get(0)._1;
        }
        if (bottomPercentile >= 0) {
            bottom = percentiles.lookup((long) (count * bottomPercentile / 100.D)).get(0)._1;
        }

        final double _top = top, _bottom = bottom;
        final byte _topPercentile = topPercentile, _bottomPercentile = bottomPercentile;
        JavaRDD<Object> outputRDD = percentiles
                .filter(t -> {
                    boolean match = true;
                    if (_topPercentile >= 0) {
                        match &= t._2._1 <= _top;
                    }
                    if (_bottomPercentile >= 0) {
                        match &= t._2._1 >= _bottom;
                    }

                    return match;
                })
                .map(t -> t._2._2);

        return Collections.singletonMap(outputName, outputRDD);
    }
}
