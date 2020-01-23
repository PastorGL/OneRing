package ash.nazg.math.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.math.config.ConfigurationParameters;
import ash.nazg.math.functions.series.NormalizeFunction;
import ash.nazg.math.functions.series.SeriesFunction;
import ash.nazg.spark.Operation;
import ash.nazg.math.functions.series.StdDevFunction;
import ash.nazg.config.OperationConfig;
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

@SuppressWarnings("unused")
public class SeriesMathOperation extends Operation {
    @Description("Default upper value for the renormalization operation")
    public static final Double DEF_CALC_CONST = 100.D;

    public static final String VERB = "seriesMath";

    private String inputName;
    private char inputDelimiter;
    private String outputName;

    private Integer calcColumn;

    private SeriesFunction seriesFunc;

    @Override
    @Description("Take an CSV RDD and calculate a 'series' mathematical function over all values in a set column," +
            " treated as a Double")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.DS_CALC_COLUMN),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_CALC_FUNCTION, SeriesCalcFunction.class),
                        new TaskDescriptionLanguage.Definition(ConfigurationParameters.OP_CALC_CONST, Double.class, DEF_CALC_CONST),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                new String[]{ConfigurationParameters.GEN_RESULT}
                        )
                )
        );
    }

    @Override
    public void setConfig(OperationConfig propertiesConfig) throws InvalidConfigValueException {
        super.setConfig(propertiesConfig);


        inputName = describedProps.inputs.get(0);
        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);
        outputName = describedProps.outputs.get(0);
        char outputDelimiter = dataStreamsProps.outputDelimiter(outputName);

        String[] outputColumns = dataStreamsProps.outputColumns.get(outputName);
        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);

        int[] outputCols = new int[outputColumns.length];
        int i = 0;
        for (String outputColumn : outputColumns) {
            if (!outputColumn.equals(ConfigurationParameters.GEN_RESULT)) {
                outputCols[i++] = inputColumns.get(outputColumn);
            } else {
                outputCols[i++] = -1;
            }
        }

        String prop;

        prop = describedProps.defs.getTyped(ConfigurationParameters.DS_CALC_COLUMN);
        calcColumn = inputColumns.get(prop);

        SeriesCalcFunction cf = describedProps.defs.getTyped(ConfigurationParameters.OP_CALC_FUNCTION);

        switch (cf) {
            case STDDEV: {
                seriesFunc = new StdDevFunction(inputDelimiter, outputDelimiter, outputCols, calcColumn);
                break;
            }
            case FIT:
            case RENORMALIZE:
            case NORMALIZE: {
                Double upper = describedProps.defs.getTyped(ConfigurationParameters.OP_CALC_CONST);
                seriesFunc = new NormalizeFunction(inputDelimiter, outputDelimiter, outputCols, calcColumn, upper);
                break;
            }
        }

    }

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

                        ret.add(new Double(row[_calcColumn]));
                    }
                    return ret.iterator();
                });

        seriesFunc.calcSeries(series);

        JavaRDD<Text> output = inputRDD.mapPartitions(seriesFunc);

        return Collections.singletonMap(outputName, output);
    }

    public enum SeriesCalcFunction {
        @Description("Calculate Standard Deviation of a value")
        STDDEV,
        @Description("Re-normalize value into a range of 0.." + ConfigurationParameters.OP_CALC_CONST)
        FIT, @Description("Alias of FIT") RENORMALIZE, @Description("Alias of FIT") NORMALIZE,
    }
}
