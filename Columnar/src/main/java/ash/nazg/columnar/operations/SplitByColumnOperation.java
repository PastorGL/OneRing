/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.columnar.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.NamedStreamsMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.storage.StorageLevel;

import java.util.*;

import static ash.nazg.config.tdl.StreamType.*;

@SuppressWarnings("unused")
public class SplitByColumnOperation extends Operation {
    public static final String OP_SPLIT_TEMPLATE = "split.template";
    public static final String RDD_OUTPUT_SPLITS_TEMPLATE = "template";
    public static final String RDD_OUTPUT_DISTINCT_SPLITS = "distinct_splits";
    public static final String DS_SPLIT_COLUMN = "split.column";

    private String inputName;
    private String outputNameTemplate;
    private String outputDistinctSplits;

    private int splitColumn;
    private String splitColumnName;
    private char inputDelimiter;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("splitByColumn", "Take a CSV RDD and split it into several RDDs" +
                " by selected column value. Output 'template' name is treated as a template for a set of" +
                " generated outputs that can reference to encountered unique values of a selected column",

                new PositionalStreamsMetaBuilder()
                        .ds("Input CSV to split into different outputs by column value",
                                new StreamType[]{CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_SPLIT_COLUMN, "If set, split by date of month column value")
                        .def(OP_SPLIT_TEMPLATE, "Template for output names wildcard part in form of 'prefix{split.column}suffix'")
                        .build(),

                new NamedStreamsMetaBuilder()
                        .ds(RDD_OUTPUT_SPLITS_TEMPLATE, "Template output with a wildcard part, i.e. output_*",
                                new StreamType[]{Passthru}, true
                        )
                        .ds(RDD_OUTPUT_DISTINCT_SPLITS, "Optional output that contains all the distinct splits occurred on the input data," +
                                        " in the form of names of the generated inputs",
                                new StreamType[]{Fixed}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputDistinctSplits = opResolver.namedOutput(RDD_OUTPUT_DISTINCT_SPLITS);
        outputNameTemplate = opResolver.namedOutput(RDD_OUTPUT_SPLITS_TEMPLATE)
                .replace("*", opResolver.definition(OP_SPLIT_TEMPLATE));

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);

        splitColumnName = opResolver.definition(DS_SPLIT_COLUMN);
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
