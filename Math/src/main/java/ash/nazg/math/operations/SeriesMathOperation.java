/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.operations;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.data.DataStream;
import ash.nazg.data.Record;
import ash.nazg.data.StreamType;
import ash.nazg.math.config.SeriesMath;
import ash.nazg.math.functions.series.SeriesFunction;
import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.metadata.OperationMeta;
import ash.nazg.metadata.Origin;
import ash.nazg.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.scripting.Operation;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;

@SuppressWarnings("unused")
public class SeriesMathOperation extends Operation {
    public static final String CALC_COLUMN = "calc.column";
    public static final String CALC_FUNCTION = "calc.function";
    public static final String CALC_CONST = "calc.const";

    public static final String GEN_RESULT = "_result";

    private String calcColumn;

    private SeriesFunction seriesFunc;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("seriesMath", "Take a DataStream and calculate a 'series' mathematical function" +
                " over all values in a set property, treated as a Double",

                new PositionalStreamsMetaBuilder()
                        .input("DataStream with a property of type Double",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Polygon, StreamType.Track}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(CALC_COLUMN, "Column with a Double to use as series source")
                        .def(CALC_FUNCTION, "The series function to perform", SeriesMath.class)
                        .def(CALC_CONST, "An optional floor value for the normalization function", Double.class,
                                100.D, "Default upper value for the renormalization operation")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("DataStream augmented with calculation result property",
                                new StreamType[]{StreamType.Columnar, StreamType.Point, StreamType.Polygon, StreamType.Track}, Origin.AUGMENTED, null
                        )
                        .generated(GEN_RESULT, "Generated property with a result of the mathematical function")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigurationException {
        calcColumn = params.get(CALC_COLUMN);
        SeriesMath seriesMath = params.get(CALC_FUNCTION);
        Double calcConst = params.get(CALC_CONST);

        try {
            seriesFunc = seriesMath.function(calcColumn, calcConst);
        } catch (Exception e) {
            throw new InvalidConfigurationException("Unable to instantiate requested function of 'seriesMath'", e);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, DataStream> execute() {
        final String _calcColumn = calcColumn;

        DataStream input = inputStreams.getValue(0);
        JavaRDD<Object> inputRDD = (JavaRDD<Object>) input.get();
        JavaDoubleRDD series = inputRDD
                .mapPartitionsToDouble(it -> {
                    List<Double> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Record row = (Record) it.next();

                        ret.add(row.asDouble(_calcColumn));
                    }
                    return ret.iterator();
                });

        seriesFunc.calcSeries(series);
        JavaRDD<Object> output = inputRDD.mapPartitions(seriesFunc);

        Map<String, List<String>> outColumns = new HashMap<>(input.accessor.attributes());
        List<String> valueColumns = new ArrayList<>(outColumns.get("value"));
        valueColumns.add(GEN_RESULT);
        outColumns.put("value", valueColumns);

        return Collections.singletonMap(outputStreams.firstKey(), new DataStream(input.streamType, output, outColumns));
    }

}
