/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.populations.functions.CountUniquesFunction;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static ash.nazg.populations.config.ConfigurationParameters.DS_COUNT_COLUMN;
import static ash.nazg.populations.config.ConfigurationParameters.DS_VALUE_COLUMN;

@SuppressWarnings("unused")
public class CountUniquesOperation extends PopulationIndicatorOperation {
    @Override
    public OperationMeta meta() {
        return new OperationMeta("countUniques", "Statistical indicator for counting unique values in a column per some other column",

                new PositionalStreamsMetaBuilder()
                        .ds("",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_COUNT_COLUMN, "Column to count unique values of other column")
                        .def(DS_VALUE_COLUMN, "Column for counting unique values per other column")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("",
                                new StreamType[]{StreamType.Fixed}
                        )
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        super.configure();

        inputValuesName = opResolver.positionalInput(0);
        inputValuesDelimiter = dsResolver.inputDelimiter(inputValuesName);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputValuesName);
        String prop;

        prop = opResolver.definition(DS_COUNT_COLUMN);
        countColumn = inputColumns.get(prop);

        prop = opResolver.definition(DS_VALUE_COLUMN);
        valueColumn = inputColumns.get(prop);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _outputDelimiter = outputDelimiter;

        JavaPairRDD<Text, Integer> userSetPerGid = new CountUniquesFunction(inputValuesDelimiter, countColumn, valueColumn)
                .call((JavaRDD<Object>) input.get(inputValuesName));

        JavaRDD<Text> output = userSetPerGid.mapPartitions(it -> {
            List<Text> ret = new ArrayList<>();

            while (it.hasNext()) {
                Tuple2<Text, Integer> t = it.next();

                String[] acc = new String[]{t._1.toString(), Integer.toString(t._2)};

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
