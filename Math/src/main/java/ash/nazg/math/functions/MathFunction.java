package ash.nazg.math.functions;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.opencsv.CSVWriter.DEFAULT_ESCAPE_CHARACTER;
import static com.opencsv.CSVWriter.DEFAULT_QUOTE_CHARACTER;

public abstract class MathFunction implements FlatMapFunction<Iterator<Object>, Text> {
    protected final int[] outputColumns;
    protected final char inputDelimiter;
    protected final char outputDelimiter;

    public MathFunction(char outputDelimiter, char inputDelimiter, int[] outputColumns) {
        this.outputColumns = outputColumns;
        this.inputDelimiter = inputDelimiter;
        this.outputDelimiter = outputDelimiter;
    }

    public abstract String[] calcLine(String[] row);

    @Override
    final public Iterator<Text> call(Iterator<Object> it) throws Exception {
        CSVParser parser = new CSVParserBuilder().withSeparator(inputDelimiter).build();

        List<Text> ret = new ArrayList<>();
        while (it.hasNext()) {
            String l = String.valueOf(it.next());
            String[] row = parser.parseLine(l);

            StringWriter buffer = new StringWriter();
            CSVWriter writer = new CSVWriter(buffer, outputDelimiter, DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER, "");

            writer.writeNext(calcLine(row), false);
            writer.close();

            ret.add(new Text(buffer.toString()));
        }

        return ret.iterator();
    }
}
