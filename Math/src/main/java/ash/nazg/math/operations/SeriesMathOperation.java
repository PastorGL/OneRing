/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionEnum;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.math.functions.series.NormalizeFunction;
import ash.nazg.math.functions.series.SeriesFunction;
import ash.nazg.math.functions.series.StdDevFunction;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ash.nazg.math.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class SeriesMathOperation extends Operation {
    private String inputName;
    private char inputDelimiter;
    private Integer calcColumn;

    private String outputName;

    private SeriesFunction seriesFunc;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("seriesMath", "Take an CSV RDD and calculate a 'series' mathematical function" +
                " over all values in a set column, treated as a Double",

                new PositionalStreamsMetaBuilder()
                        .ds("",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_CALC_COLUMN, "Column with a Double to use as series source")
                        .def(OP_CALC_FUNCTION, "The series function to perform", SeriesCalcFunction.class)
                        .def(OP_CALC_CONST, "An optional floor value for the normalization function", Double.class,
                                "100", "Default upper value for the renormalization operation")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .genCol(GEN_RESULT, "Generated column with a result of the mathematical function")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputName = opResolver.positionalOutput(0);
        char outputDelimiter = dsResolver.outputDelimiter(outputName);

        String[] outputColumns = dsResolver.outputColumns(outputName);
        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);

        int[] outputCols = new int[outputColumns.length];
        int i = 0;
        for (String outputColumn : outputColumns) {
            if (!outputColumn.equals(GEN_RESULT)) {
                outputCols[i++] = inputColumns.get(outputColumn);
            } else {
                outputCols[i++] = -1;
            }
        }

        String prop;

        prop = opResolver.definition(DS_CALC_COLUMN);
        calcColumn = inputColumns.get(prop);

        SeriesCalcFunction cf = opResolver.definition(OP_CALC_FUNCTION);

        switch (cf) {
            case STDDEV: {
                seriesFunc = new StdDevFunction(inputDelimiter, outputDelimiter, outputCols, calcColumn);
                break;
            }
            case NORMALIZE: {
                Double upper = opResolver.definition(OP_CALC_CONST);
                seriesFunc = new NormalizeFunction(inputDelimiter, outputDelimiter, outputCols, calcColumn, upper);
                break;
            }
        }

    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter = inputDelimiter;
        final Integer _calcColumn = calcColumn;

        JavaRDD<Object> inputRDD = (JavaRDD<Object>) input.get(inputName);

        JavaDoubleRDD series = inputRDD
                .mapPartitionsToDouble(it -> {
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

                    List<Double> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Object o = it.next();

                        String l = o instanceof String ? (String) o : String.valueOf(o);
                        String[] row = parser.parseLine(l);

                        ret.add(Double.parseDouble(row[_calcColumn]));
                    }
                    return ret.iterator();
                });

        seriesFunc.calcSeries(series);

        JavaRDD<Text> output = inputRDD.mapPartitions(seriesFunc);

        return Collections.singletonMap(outputName, output);
    }

    public enum SeriesCalcFunction implements DefinitionEnum {
        STDDEV("Calculate Standard Deviation of a value"),
        NORMALIZE("Re-normalize value into a range of 0.." + OP_CALC_CONST);

        private final String descr;

        SeriesCalcFunction(String descr) {
            this.descr = descr;
        }

        @Override
        public String descr() {
            return descr;
        }
    }
}
