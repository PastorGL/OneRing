/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.transform;

import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.data.*;
import ash.nazg.metadata.TransformMeta;
import ash.nazg.metadata.TransformedStreamMetaBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class PairToTextTransform implements Transform {
    @Override
    public TransformMeta meta() {
        return new TransformMeta("pairToText", StreamType.KeyValue, StreamType.PlainText,
                "Converts Columnar DataStream to plain text",

                new DefinitionMetaBuilder()
                        .def("delimiter", "Tabular data delimiter", "\t", "By default, tab character")
                        .build(),
                new TransformedStreamMetaBuilder()
                        .genCol("_key", "Key of the Pair")
                        .build()
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

            return new DataStream(StreamType.PlainText, ((JavaPairRDD<Text, Columnar>) ds.get())
                    .mapPartitions(it -> {
                        List<Text> ret = new ArrayList<>();

                        while (it.hasNext()) {
                            Tuple2<Text, Columnar> o = it.next();
                            Columnar line = o._2;

                            StringWriter buffer = new StringWriter();
                            CSVWriter writer = new CSVWriter(buffer, delimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                            String[] columns = new String[len];
                            for (int i = 0; i < len; i++) {
                                String key = _outputColumns.get(i);
                                columns[i] = key.equalsIgnoreCase("_key") ? String.valueOf(o._1) : String.valueOf(line.asIs(key));
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
