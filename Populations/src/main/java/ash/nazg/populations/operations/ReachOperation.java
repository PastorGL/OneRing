package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.*;

import static ash.nazg.populations.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class ReachOperation extends PopulationIndicatorOperation {
    private static final String VERB = "reach";

    private String inputPopulationName;

    @Override
    @Description("Statistical indicator for some audience reach")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_VALUES_COUNT_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_VALUES_VALUE_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_INPUT_VALUES,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_INPUT_POPULATION,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
                                        false
                                ),
                        }
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Fixed},
                                false
                        )
                )
        );
    }

    @Override
    public void setConfig(OperationConfig propertiesConfig) throws InvalidConfigValueException {
        super.setConfig(propertiesConfig);

        inputValuesName = describedProps.namedInputs.get(RDD_INPUT_VALUES);
        inputValuesDelimiter = dataStreamsProps.inputDelimiter(inputValuesName);

        inputPopulationName = describedProps.namedInputs.get(RDD_INPUT_POPULATION);

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputValuesName);
        String prop;

        prop = describedProps.defs.getTyped(DS_VALUES_VALUE_COLUMN);
        valueColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(DS_VALUES_COUNT_COLUMN);
        countColumn = inputColumns.get(prop);
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _outputDelimiter = outputDelimiter;

        final long N = input.get(inputPopulationName).count();

        JavaPairRDD<Text, Set<Text>> userSetPerGid = new ValueSetPerCountColumn(inputValuesDelimiter, countColumn, valueColumn)
                .call((JavaRDD<Object>) input.get(inputValuesName));

        JavaRDD<Text> output = userSetPerGid.mapPartitions(it -> {
            List<Text> ret = new ArrayList<>();

            while (it.hasNext()) {
                Tuple2<Text, Set<Text>> t = it.next();

                String[] acc = new String[]{t._1.toString(), Double.toString((double) t._2.size() / N)};

                StringWriter buffer = new StringWriter();
                CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                writer.writeNext(acc, false);
                writer.close();

                ret.add(new Text(buffer.toString()));
            }

            return ret.iterator();
        });

        return Collections.singletonMap(outputName, output);
    }
}
