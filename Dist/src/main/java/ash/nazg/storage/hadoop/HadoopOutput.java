/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.storage.OutputAdapter;
import ash.nazg.storage.StorageAdapter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;
import java.util.regex.Pattern;

public class HadoopOutput extends OutputAdapter {
    protected String codec;
    protected String[] columns;
    protected char delimiter;

    @Description("Default Storage that utilizes Hadoop filesystems")
    public Pattern proto() {
        return StorageAdapter.PATH_PATTERN;
    }

    protected void configure() throws InvalidConfigValueException {
        codec = outputResolver.get("codec", "none");

        columns = dsResolver.outputColumns(name);
        delimiter = dsResolver.outputDelimiter(name);
    }

    @Override
    public void save(String path, JavaRDD<Text> rdd) {
        Function2<Integer, Iterator<Text>, Iterator<Void>> outputFunction = new PartOutputFunction(name, path, codec, columns, delimiter);

        rdd.mapPartitionsWithIndex(outputFunction, true).count();
    }
}
