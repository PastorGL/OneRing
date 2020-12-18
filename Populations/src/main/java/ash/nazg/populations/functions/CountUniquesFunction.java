/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.functions;

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

public class CountUniquesFunction implements Function<JavaRDD<Object>, JavaPairRDD<Text, Integer>> {
    private final char inputDelimiter;
    private final int countColumn;
    private final int valueColumn;

    public CountUniquesFunction(char inputDelimiter, int countColumn, int valueColumn) {
        this.inputDelimiter = inputDelimiter;
        this.valueColumn = valueColumn;
        this.countColumn = countColumn;
    }

    public JavaPairRDD<Text, Integer> call(JavaRDD<Object> input) {
        return input.mapPartitionsToPair(it -> {
            CSVParser parser = new CSVParserBuilder()
                    .withSeparator(inputDelimiter).build();

            List<Tuple2<Text, Text>> ret = new ArrayList<>();
            while (it.hasNext()) {
                Object o = it.next();
                String l = o instanceof String ? (String) o : String.valueOf(o);

                String[] row = parser.parseLine(l);

                Text value = new Text(row[valueColumn]);
                Text count = new Text(row[countColumn]);

                ret.add(new Tuple2<>(count, value));
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
        ).mapToPair(t -> new Tuple2<>(t._1, t._2.size()));
    }
}
