package ash.nazg.columnar.operations;

import ash.nazg.spark.Operation;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.storage.StorageLevel;

import java.util.*;

import static ash.nazg.config.tdl.TaskDescriptionLanguage.StreamType.*;

@SuppressWarnings("unused")
public class SplitByColumnOperation extends Operation {
    @Description("Template for output names wildcard part in form of 'prefix{split.column}suffix'")
    public static final String OP_SPLIT_TEMPLATE = "split.template";
    @Description("Template output with a wildcard part, i.e. output_*")
    public static final String RDD_OUTPUT_SPLITS_TEMPLATE = "template";
    @Description("Optional output that contains all the distinct splits occurred on the input data," +
            " in the form of names of the generated inputs")
    public static final String RDD_OUTPUT_DISTINCT_SPLITS = "distinct_splits";
    @Description("If set, split by date of month column value")
    public static final String DS_SPLIT_COLUMN = "split.column";

    public static final String VERB = "splitByColumn";

    private String inputName;
    private String outputNameTemplate;
    private String outputDistinctSplits;

    private int splitColumn;
    private String splitColumnName;
    private char inputDelimiter;

    @Override
    @Description("Take a CSV RDD and split it into several RDDs" +
            " by selected column value. Output 'template' name is treated as a template for a set of" +
            " generated outputs that can reference to encountered unique values of a selected column")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_SPLIT_COLUMN),
                        new TaskDescriptionLanguage.Definition(OP_SPLIT_TEMPLATE),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_OUTPUT_SPLITS_TEMPLATE,
                                        new TaskDescriptionLanguage.StreamType[]{Passthru},
                                        true
                                ),
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_OUTPUT_DISTINCT_SPLITS,
                                        new TaskDescriptionLanguage.StreamType[]{Fixed},
                                        false
                                )
                        }
                )
        );
    }

    @Override
    public void setConfig(OperationConfig propertiesConfig) throws InvalidConfigValueException {
        super.setConfig(propertiesConfig);

        inputName = describedProps.inputs.get(0);
        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);
        outputDistinctSplits = describedProps.namedOutputs.get(RDD_OUTPUT_DISTINCT_SPLITS);
        outputNameTemplate = describedProps.namedOutputs.get(RDD_OUTPUT_SPLITS_TEMPLATE)
                .replace("*", describedProps.defs.getTyped(OP_SPLIT_TEMPLATE));

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);

        splitColumnName = describedProps.defs.getTyped(DS_SPLIT_COLUMN);
        splitColumn = inputColumns.get(splitColumnName);

        if (!outputNameTemplate.contains("{" + splitColumnName + "}")) {
            throw new InvalidConfigValueException("Split output name template '" + outputNameTemplate + "' must include split column reference {" + splitColumnName + "} for the operation '" + name + "'");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        Map<String, JavaRDDLike> output = new HashMap<>();

        JavaRDD<Object> cachedInput = ((JavaRDD<Object>) input.get(inputName))
                .persist(StorageLevel.MEMORY_AND_DISK_SER());

        char _inputDelimiter = inputDelimiter;
        int _splitColumn = splitColumn;

        JavaRDD<Text> distinctSplits = cachedInput
                .mapPartitions(it -> {
                    Set<Text> ret = new HashSet<>();

                    CSVParser parser = new CSVParserBuilder()
                            .withSeparator(_inputDelimiter).build();

                    while (it.hasNext()) {
                        Object v = it.next();
                        String l = v instanceof String ? (String) v : String.valueOf(v);

                        String[] ll = parser.parseLine(l);

                        ret.add(new Text(ll[_splitColumn]));
                    }

                    return ret.iterator();
                })
                .distinct();

        if (outputDistinctSplits != null) {
            output.put(outputDistinctSplits, distinctSplits);
        }

        List<Text> uniques = distinctSplits
                .collect();

        for (Text u : uniques) {
            final String _splitValue = String.valueOf(u);

            String splitName = outputNameTemplate.replace("{" + splitColumnName + "}", _splitValue);

            JavaRDD<Object> split = cachedInput.mapPartitions(it -> {
                List ret = new ArrayList<>();

                CSVParser parser = new CSVParserBuilder()
                        .withSeparator(_inputDelimiter).build();

                while (it.hasNext()) {
                    Object v = it.next();
                    String l = v instanceof String ? (String) v : String.valueOf(v);

                    String[] ll = parser.parseLine(l);

                    if (ll[_splitColumn].equals(_splitValue)) {
                        ret.add(v);
                    }
                }

                return ret.iterator();
            });

            output.put(splitName, split);
        }

        return output;
    }
}
