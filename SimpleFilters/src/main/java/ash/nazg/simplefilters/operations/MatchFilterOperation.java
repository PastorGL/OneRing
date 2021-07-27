/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

import static ash.nazg.simplefilters.config.ConfigurationParameters.*;

public abstract class MatchFilterOperation extends Operation {
    protected char inputSourceDelimiter;
    protected String inputSourceName;
    protected int matchColumn;

    protected String outputMatchedName;
    protected String outputEvictedName;

    @Override
    public void configure() throws InvalidConfigValueException {
        inputSourceName = opResolver.namedInput(RDD_INPUT_SOURCE);
        inputSourceDelimiter = dsResolver.inputDelimiter(inputSourceName);

        outputMatchedName = opResolver.namedOutput(RDD_OUTPUT_MATCHED);
        outputEvictedName = opResolver.namedOutput(RDD_OUTPUT_EVICTED);

        Map<String, Integer> inputSourceColumns = dsResolver.inputColumns(inputSourceName);

        String prop = opResolver.definition(DS_SOURCE_MATCH_COLUMN);
        matchColumn = inputSourceColumns.get(prop);
    }

    public static class MatchFunction implements PairFlatMapFunction<Iterator<Object>, Boolean, Object> {
        private final char inputDelimiter;
        private final Broadcast<HashSet<String>> bMatchSet;
        private final int matchColumn;

        public MatchFunction(Broadcast<HashSet<String>> bMatchSet, char inputDelimiter, int matchColumn) {
            this.inputDelimiter = inputDelimiter;
            this.bMatchSet = bMatchSet;
            this.matchColumn = matchColumn;
        }

        @Override
        public Iterator<Tuple2<Boolean, Object>> call(Iterator<Object> it) throws Exception {
            CSVParser parser = new CSVParserBuilder().withSeparator(inputDelimiter).build();

            HashSet<String> _matchSet = bMatchSet.getValue();

            List<Tuple2<Boolean, Object>> ret = new ArrayList<>();
            while (it.hasNext()) {
                Object v = it.next();
                String l = v instanceof String ? (String) v : String.valueOf(v);

                String m = parser.parseLine(l)[matchColumn];

                ret.add(new Tuple2<>(_matchSet.contains(m), v));
            }

            return ret.iterator();
        }
    }
}
