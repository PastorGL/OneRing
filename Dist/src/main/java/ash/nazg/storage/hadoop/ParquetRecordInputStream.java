/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import com.opencsv.CSVWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

public class ParquetRecordInputStream extends RecordInputStream {
    private final ParquetReader<Group> reader;

    public ParquetRecordInputStream(ParquetReader<Group> reader, int[] fieldOrder, char delimiter) {
        super(fieldOrder, delimiter);
        this.reader = reader;
    }

    @Override
    protected void ensureRecord() throws IOException {
        if (position == size) {
            Group g = reader.read();

            if (g == null) {
                recordBuffer = null;
            } else {
                String[] acc = new String[order.length];

                for (int i = 0; i < order.length; i++) {
                    int l = order[i];
                    acc[i] = g.getValueToString(l, 0);
                }

                StringWriter stringBuffer = new StringWriter();
                CSVWriter writer = new CSVWriter(stringBuffer, delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
                writer.writeNext(acc, false);
                writer.close();

                recordBuffer = stringBuffer.toString().getBytes(StandardCharsets.UTF_8);

                position = 0;
                size = recordBuffer.length;
            }
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
