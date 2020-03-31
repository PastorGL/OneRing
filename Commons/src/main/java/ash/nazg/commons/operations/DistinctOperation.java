/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;

@SuppressWarnings("unused")
public class DistinctOperation extends Operation {
    @Description("Input column that contains a value treated as a distinction key")
    public static final String DS_UNIQUE_COLUMN = "unique.column";
    @Description("By default, no unique column is set for an RDD")
    public static final String DEF_UNIQUE_COLUMN = null;

    public static final String VERB = "distinct";

    private String inputName;
    private String outputName;
    private char inputDelimiter;
    private Integer uniqueColumn;

    @Override
    @Description("If input is an CSV RDD, this operation takes a column and extract a list of distinct values occurred in this column." +
            " If input is a PairRDD, it returns distinct elements from this PairRDD.")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_UNIQUE_COLUMN, String.class, DEF_UNIQUE_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.KeyValue, TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Passthru},
                                false
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.inputs.get(0);
        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);
        outputName = describedProps.outputs.get(0);

        String uniqueColumn = describedProps.defs.getTyped(DS_UNIQUE_COLUMN);
        if (uniqueColumn != null) {
            Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);
            this.uniqueColumn = inputColumns.get(uniqueColumn);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        Object inp = input.get(inputName);

        if (inp instanceof JavaRDD) {
            char _inputDelimiter = inputDelimiter;
            int _uniqueColumn = uniqueColumn;

            JavaRDD<Text> out = ((JavaRDD<Object>) inp).mapPartitions(it -> {
                CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

                List<Text> ret = new ArrayList<>();
                while (it.hasNext()) {
                    Object v = it.next();
                    String l = v instanceof String ? (String) v : String.valueOf(v);

                    String[] ll = parser.parseLine(l);

                    ret.add(new Text(ll[_uniqueColumn]));
                }

                return ret.iterator();
            }).distinct();

            return Collections.singletonMap(outputName, out);
        }

        if (inp instanceof JavaPairRDD) {
            JavaPairRDD out = ((JavaPairRDD) inp)
                    .distinct();

            return Collections.singletonMap(outputName, out);
        }

        return null;
    }
}
