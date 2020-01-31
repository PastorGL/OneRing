/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.WrapperConfig;
import ash.nazg.cli.TaskWrapper;
import ash.nazg.cli.config.TaskWrapperConfig;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static ash.nazg.config.WrapperConfig.DS_INPUT_PATH_PREFIX;

public class TestTaskWrapper extends TaskWrapper implements AutoCloseable {
    private static SparkConf sparkConf = new SparkConf()
            .setAppName("test")
            .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName())
            .setMaster("local[*]")
            .set("spark.network.timeout", "10000")
            .set("spark.ui.enabled", "false");

    public TestTaskWrapper(boolean replpath, String path) {
        super(new JavaSparkContext(sparkConf), new TaskWrapperConfig());
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

    public Map<String, JavaRDDLike> getResult() {
        return result;
    }

    public void close() {
        context.stop();
    }
}
