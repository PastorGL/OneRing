/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.input;

import ash.nazg.config.DataStreamsConfig;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.storage.AerospikeAdapter;
import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.InputAdapter;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.AeroRDD;
import scala.reflect.ClassManifestFactory$;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class AerospikeInput extends AerospikeAdapter implements InputAdapter {
    private static final Pattern PATTERN = Pattern.compile("^aero:SELECT.+");

    private JavaSparkContext ctx;
    private int partCount;

    @Override
    public void setProperties(String inputName, WrapperConfig wrapperConfig) throws InvalidConfigValueException {
        aerospikeHost = wrapperConfig.getInputProperty("aerospike.host", "localhost");
        aerospikePort = Integer.valueOf(wrapperConfig.getInputProperty("aerospike.port", "3000"));

        partCount = wrapperConfig.inputParts(inputName);

        DataStreamsConfig adapterConfig = new DataStreamsConfig(wrapperConfig.getProperties(), Collections.singleton(inputName), Collections.singleton(inputName), null, null, null);

        delimiter = adapterConfig.inputDelimiter(inputName);
    }

    @Override
    public Pattern proto() {
        return PATTERN;
    }

    @Override
    public void setContext(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public JavaRDDLike load(String path) {
        final char _inputDelimiter = delimiter;

        return new AeroRDD<Object[]>(
                ctx.sc(),
                aerospikeHost,
                aerospikePort,
                path.split(":", 2)[1],
                ClassManifestFactory$.MODULE$.fromClass(Object[].class)
        ).toJavaRDD()
                .repartition(Math.max(partCount, 1))
                .mapPartitions(it -> {
                    List<Text> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Object[] v = it.next();

                        String[] acc = new String[v.length];

                        int i = 0;
                        for (Object col : v) {
                            acc[i++] = String.valueOf(col);
                        }

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _inputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(acc, false);
                        writer.close();

                        ret.add(new Text(buffer.toString()));
                    }

                    return ret.iterator();
                });
    }
}
