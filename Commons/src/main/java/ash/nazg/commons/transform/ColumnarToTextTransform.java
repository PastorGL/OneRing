/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class ColumnarToTextTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("columnarToText", StreamType.Columnar, StreamType.PlainText,
                "Converts Columnar DataStream to plain text",

                new DefinitionMetaBuilder()
                        .def("delimiter", "Tabular data delimiter", "\t", "By default, tab character")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final char delimiter = ((String) params.get("delimiter")).charAt(0);

            List<String> valueColumns = newColumns.get("value");
            if (valueColumns == null) {
                valueColumns = ds.accessor.attributes("value");
            }

            List<String> _outputColumns = valueColumns;
            int len = _outputColumns.size();

            return new DataStream(StreamType.PlainText, ((JavaRDD<Columnar>) ds.get())
                    .mapPartitions(it -> {
                        List<Text> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Columnar line = it.next();

                            StringWriter buffer = new StringWriter();
                            CSVWriter writer = new CSVWriter(buffer, delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                            String[] columns = new String[len];
                            for (int i = 0; i < len; i++) {
                                columns[i] = String.valueOf(line.asIs(_outputColumns.get(i)));
                            }
                            writer.writeNext(columns, false);
                            writer.close();

                            ret.add(new Text(buffer.toString()));
                        }

                        return ret.iterator();
                    }), null);
        };
    }
}
