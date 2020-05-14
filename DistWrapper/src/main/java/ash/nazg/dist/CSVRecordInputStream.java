/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.commons.io.Charsets;

import java.io.*;

public class CSVRecordInputStream extends RecordInputStream {
    private final BufferedReader reader;
    private final CSVParser parser;

    public CSVRecordInputStream(InputStream input, int[] columnOrder, char delimiter) {
        super(columnOrder, delimiter);
        this.reader = new BufferedReader(new InputStreamReader(input));
        this.parser = new CSVParserBuilder().withSeparator(delimiter).build();
    }

    protected void ensureRecord() throws IOException {
        if (position == size) {
            String line = reader.readLine();

            if (line == null) {
                recordBuffer = null;
            } else {
                position = 0;

                try {
                    String[] ll = parser.parseLine(line);
                    String[] acc = new String[order.length];

                    for (int i = 0; i < order.length; i++) {
                        int l = order[i];
                        acc[i] = ll[l];
                    }

                    StringWriter stringBuffer = new StringWriter();
                    CSVWriter writer = new CSVWriter(stringBuffer, delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                            CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
                    writer.writeNext(acc, false);
                    writer.close();

                    recordBuffer = stringBuffer.toString().getBytes(Charsets.UTF_8);
                    size = recordBuffer.length;
                } catch (Exception e) {
                    size = 0;

                    System.err.println("Malformed input line: " + line);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
