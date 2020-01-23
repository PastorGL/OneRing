package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.config.OperationConfig;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.*;

import static ash.nazg.populations.config.ConfigurationParameters.DS_COUNT_COLUMN;
import static ash.nazg.populations.config.ConfigurationParameters.DS_VALUE_COLUMN;

@SuppressWarnings("unused")
public class CountUniquesOperation extends PopulationIndicatorOperation {
    private static final String VERB = "countUniques";

    @Override
    @Description("Statistical indicator for counting unique values in a column per some other column")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_COUNT_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_VALUE_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
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

        inputValuesName = describedProps.inputs.get(0);
        inputValuesDelimiter = dataStreamsProps.inputDelimiter(inputValuesName);

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputValuesName);
        String prop;

        prop = describedProps.defs.getTyped(DS_COUNT_COLUMN);
        countColumn = inputColumns.get(prop);

        prop = describedProps.defs.getTyped(DS_VALUE_COLUMN);
        valueColumn = inputColumns.get(prop);
    }


    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _outputDelimiter = outputDelimiter;

        JavaPairRDD<Text, Set<Text>> userSetPerGid = new ValueSetPerCountColumn(inputValuesDelimiter, countColumn, valueColumn)
                .call((JavaRDD<Object>) input.get(inputValuesName));

        JavaRDD<Text> output = userSetPerGid.mapPartitions(it -> {
            List<Text> ret = new ArrayList<>();

            while (it.hasNext()) {
                Tuple2<Text, Set<Text>> t = it.next();

                String[] acc = new String[]{t._1.toString(), Long.toString(t._2.size())};

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
