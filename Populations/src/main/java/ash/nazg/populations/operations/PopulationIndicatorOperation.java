/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class PopulationIndicatorOperation extends Operation {
    protected int countColumn;
    protected int valueColumn;

    protected String inputValuesName;
    protected char inputValuesDelimiter;

    protected String outputName;
    protected char outputDelimiter;

    @Override
    public void setConfig(OperationConfig config) throws InvalidConfigValueException {
        super.setConfig(config);

        outputName = describedProps.outputs.get(0);
        outputDelimiter = dataStreamsProps.outputDelimiter(outputName);
    }

    protected static class ValueSetPerCountColumn implements Function<JavaRDD<Object>, JavaPairRDD<Text, Set<Text>>> {
        private char inputDelimiter;
        private int countColumn;
        private int valueColumn;

        public ValueSetPerCountColumn(char inputDelimiter, int countColumn, int valueColumn) {
            this.inputDelimiter = inputDelimiter;
            this.valueColumn = valueColumn;
            this.countColumn = countColumn;
        }

        public JavaPairRDD<Text, Set<Text>> call(JavaRDD<Object> input) {
            return input.mapPartitionsToPair(it -> {
                CSVParser parser = new CSVParserBuilder()
                        .withSeparator(inputDelimiter).build();

                List<Tuple2<Text, Text>> ret = new ArrayList<>();
                while (it.hasNext()) {
                    Object o = it.next();
                    String l = o instanceof String ? (String) o : String.valueOf(o);

                    String[] row = parser.parseLine(l);

                    Text userid = new Text(row[valueColumn]);
                    Text gid = new Text(row[countColumn]);

                    ret.add(new Tuple2<>(gid, userid));
                }

                return ret.iterator();
            }).combineByKey(
                    t -> {
                        Set<Text> s = new HashSet<>();
                        s.add(t);
                        return s;
                    },
                    (c, t) -> {
                        c.add(t);
                        return c;
                    },
                    (c1, c2) -> {
                        c1.addAll(c2);
                        return c1;
                    }
            );
        }
    }
}
