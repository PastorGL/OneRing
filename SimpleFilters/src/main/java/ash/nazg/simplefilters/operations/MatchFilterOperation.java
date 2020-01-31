/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public abstract class MatchFilterOperation extends Operation {
    protected String inputName;
    protected char inputDelimiter;
    protected int matchColumn;
    protected String outputName;

    @Override
    public void setConfig(OperationConfig config) throws InvalidConfigValueException {
        super.setConfig(config);

        outputName = describedProps.outputs.get(0);
    }

    public static class MatchFunction implements FlatMapFunction<Iterator<Object>, Object> {
        private char inputDelimiter;
        private Broadcast<HashSet<String>> bMatchSet;
        private int matchColumn;

        public MatchFunction(Broadcast<HashSet<String>> bMatchSet, char inputDelimiter, int matchColumn) {
            this.inputDelimiter = inputDelimiter;
            this.bMatchSet = bMatchSet;
            this.matchColumn = matchColumn;
        }

        @Override
        public Iterator<Object> call(Iterator<Object> it) throws Exception {
            CSVParser parser = new CSVParserBuilder().withSeparator(inputDelimiter).build();

            HashSet<String> _matchSet = bMatchSet.getValue();

            List<Object> ret = new ArrayList<>();
            while (it.hasNext()) {
                Object v = it.next();
                String l = v instanceof String ? (String) v : String.valueOf(v);

                String m = parser.parseLine(l)[matchColumn];
                if (_matchSet.contains(m)) {
                    ret.add(v);
                }
            }

            return ret.iterator();
        }
    }
}
