/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.input.HadoopInput;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static ash.nazg.config.WrapperConfig.DS_INPUT_PATH_PREFIX;
import static ash.nazg.config.WrapperConfig.DS_OUTPUT_PATH;

public class TestRunner extends TaskRunnerWrapper implements AutoCloseable {
    private static SparkConf sparkConf = new SparkConf()
            .setAppName("test")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .setMaster("local[*]")
            .set("spark.network.timeout", "10000")
            .set("spark.ui.enabled", "false");

    public TestRunner(String path) {
        this(path, null);
    }

    public TestRunner(String path, String varPath) {
        super(new JavaSparkContext(sparkConf), new WrapperConfig());

        context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        try (InputStream input = getClass().getResourceAsStream(path)) {
            if (varPath != null) {
                InputStream vars = getClass().getResourceAsStream(varPath);

                Properties overrides = new Properties();
                overrides.load(vars);

                wrapperConfig.setOverrides(overrides);
            }

            Properties source = new Properties();
            source.load(input);

            String rootResourcePath = getClass().getResource("/").getPath();
            for (Object p : source.keySet()) {
                String prop = (String) p;
                if (prop.startsWith(DS_INPUT_PATH_PREFIX)) {
                    source.setProperty(prop, rootResourcePath + source.get(p));
                }
            }
            source.setProperty(DS_OUTPUT_PATH, "goes to nowhere");

            wrapperConfig.setProperties(source);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, JavaRDDLike> go() throws Exception {
        Map<String, JavaRDDLike> rdds = new HashMap<>();

        HadoopInput hi = new HadoopInput();
        hi.setContext(context);
        for (String sink : wrapperConfig.getInputSink()) {
            WrapperConfig taskConfig = wrapperConfig;
            hi.setProperties(sink, taskConfig);
            rdds.put(sink, hi.load(taskConfig.inputPath(sink)));
        }

        processTaskChain(rdds);

        return rdds;
    }

    public void close() {
        context.stop();
    }
}
