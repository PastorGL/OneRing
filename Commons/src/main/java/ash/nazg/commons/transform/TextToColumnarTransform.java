/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.config.Constants;
import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class TextToColumnarTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("textToColumnar", StreamType.PlainText, StreamType.Columnar,
                "This converts PlainText delimiter-separated DataStream into Columnar",

                new DefinitionMetaBuilder()
                        .def("delimiter", "Column delimiting character", "\t", "By default, a tab character is used")
                        .build(),
                null
        );
    }

    @Override
    public StreamConverter converter() {
        return (ds, newColumns, params) -> {
            final List<String> _outputColumns = newColumns.get("value");

            final char _inputDelimiter = ((String) params.get("delimiter")).charAt(0);

            return new DataStream(StreamType.Columnar, ((JavaRDD<Object>) ds.get())
                    .mapPartitions(it -> {
                        List<Columnar> ret = new ArrayList<>();

                        CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                        while (it.hasNext()) {
                            String[] line = parser.parseLine(String.valueOf(it.next()));

                            Columnar rec = new Columnar(_outputColumns);
                            for (int i = 0, outputColumnsSize = _outputColumns.size(); i < outputColumnsSize; i++) {
                                String col = _outputColumns.get(i);
                                if (!col.equals(Constants.UNDERSCORE)) {
                                    rec.put(col, line[i]);
                                }
                            }

                            ret.add(rec);
                        }

                        return ret.iterator();
                    }), newColumns);
        };
    }
}
