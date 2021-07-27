/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.storage.OutputAdapter;
import ash.nazg.storage.metadata.AdapterMeta;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;

public class HadoopOutput extends OutputAdapter {
    protected static final String CODEC = "codec";

    protected HadoopStorage.Codec codec;
    protected String[] columns;
    protected char delimiter;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("Hadoop", "Default output adapter that utilizes Hadoop FileSystems." +
                " Supports text, text-based columnar (CSV/TSV), and Parquet files, optionally compressed",
                HadoopStorage.PATH_PATTERN,

                new DefinitionMetaBuilder()
                        .def(CODEC, "Codec to compress the output", HadoopStorage.Codec.class, HadoopStorage.Codec.NONE.name(),
                                "By default, use no compression")
                        .build()
        );
    }

    protected void configure() throws InvalidConfigValueException {
        codec = outputResolver.definition(CODEC);

        columns = dsResolver.outputColumns(dsName);
        delimiter = dsResolver.outputDelimiter(dsName);
    }

    @Override
    public void save(String path, JavaRDD<Text> rdd) {
        Function2<Integer, Iterator<Text>, Iterator<Void>> outputFunction = new PartOutputFunction(dsName, path, codec, columns, delimiter);

        rdd.mapPartitionsWithIndex(outputFunction, true).count();
    }
}
