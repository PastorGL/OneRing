/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.tdl.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestRunner implements AutoCloseable {
    private final JavaSparkContext context;
    private final TaskDefinitionLanguage.Task taskConfig;

    public TestRunner(String path) {
        this(path, null);
    }

    public TestRunner(String path, String varPath) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("One Ring Test Runner")
                .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName())
                .setMaster("local[*]")
                .set("spark.network.timeout", "10000")
                .set("spark.ui.enabled", "false");
        context = new JavaSparkContext(sparkConf);
        context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        try (InputStream input = getClass().getResourceAsStream(path)) {
            Properties overrides = new Properties();

            if (varPath != null) {
                InputStream vars = getClass().getResourceAsStream(varPath);
                overrides.load(vars);
            }

            Properties source = new Properties();
            source.load(input);

            String rootResourcePath = getClass().getResource("/").getPath();
            for (Object p : source.keySet()) {
                String prop = (String) p;
                if (prop.startsWith(Constants.DS_INPUT_PATH_PREFIX)) {
                    source.setProperty(prop, rootResourcePath + source.get(p));
                }
            }
            source.setProperty(Constants.DS_OUTPUT_PATH, "goes to nowhere");

            taskConfig = PropertiesReader.toTask(source, overrides);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, JavaRDDLike> go() throws Exception {
        Map<String, JavaRDDLike> rdds = new HashMap<>();

        StreamResolver dsResolver = new StreamResolver(taskConfig.dataStreams);

        for (String input : taskConfig.input) {
            rdds.put(input, context.textFile(dsResolver.inputPath(input), Math.max(dsResolver.inputParts(input), 1)));
        }

        new Interpreter(taskConfig, context).processTaskChain(rdds);

        return rdds;
    }

    public void close() {
        context.stop();
    }
}
