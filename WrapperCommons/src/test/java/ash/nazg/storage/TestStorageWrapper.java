/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.WrapperConfig;
import ash.nazg.spark.TaskRunnerWrapper;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static ash.nazg.config.WrapperConfig.DS_INPUT_PATH_PREFIX;

public class TestStorageWrapper extends TaskRunnerWrapper implements AutoCloseable {
    public final Map<String, JavaRDDLike> result = new HashMap<>();

    private static SparkConf sparkConf = new SparkConf()
            .setAppName("test")
            .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName())
            .setMaster("local[*]")
            .set("spark.network.timeout", "10000")
            .set("spark.ui.enabled", "false");

    public TestStorageWrapper(boolean replpath, String path) {
        super(new JavaSparkContext(sparkConf), new WrapperConfig());
        context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        try (InputStream input = getClass().getResourceAsStream(path)) {
            Properties source = new Properties();
            source.load(input);

            if (replpath) {
                String rootResourcePath = getClass().getResource("/").getPath();
                for (Object p : source.keySet()) {
                    String prop = (String) p;
                    if (prop.startsWith(DS_INPUT_PATH_PREFIX)) {
                        source.setProperty(prop, rootResourcePath + source.get(p));
                    }
                }
            }

            wrapperConfig.setProperties(source);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public WrapperConfig getConfig() {
        return wrapperConfig;
    }

    public void close() {
        context.stop();
    }

    public void go() throws Exception {
        List<String> sinks = wrapperConfig.getInputSink();
        for (String sink : sinks) {
            String path = wrapperConfig.inputPath(sink);

            InputAdapter inputAdapter = Adapters.input(path);
            inputAdapter.setContext(context);
            inputAdapter.setProperties(sink, wrapperConfig);
            result.put(sink, inputAdapter.load(path));
        }

        processTaskChain(result);

        List<String> tees = wrapperConfig.getTeeOutput();

        Set<String> rddNames = result.keySet();
        Set<String> teeNames = new HashSet<>();
        for (String tee : tees) {
            for (String name : rddNames) {
                if (name.equals(tee)) {
                    teeNames.add(name);
                }
            }
        }

        for (String teeName : teeNames) {
            JavaRDDLike rdd = result.get(teeName);

            if (rdd != null) {
                String path = wrapperConfig.outputPath(teeName);

                OutputAdapter outputAdapter = Adapters.output(path);
                outputAdapter.setProperties(teeName, wrapperConfig);
                outputAdapter.save(path, rdd);
            }
        }
    }
}
