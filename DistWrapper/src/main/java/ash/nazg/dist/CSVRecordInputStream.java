/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.commons.io.Charsets;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.Scanner;

public class CSVRecordInputStream extends RecordInputStream {
    private final Scanner scanner;
    private final CSVParser parser;

    public CSVRecordInputStream(InputStream input, int[] columnOrder, char delimiter) {
        super(columnOrder, delimiter);
        this.scanner = new Scanner(input);
        this.parser = new CSVParserBuilder().withSeparator(delimiter).build();
    }

    protected void ensureRecord() {
        if (position == size) {
            boolean hasLine = scanner.hasNextLine();

            if (!hasLine) {
                recordBuffer = null;
            } else {
                String line = scanner.nextLine();
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
    public void close() {
        scanner.close();
    }
}
