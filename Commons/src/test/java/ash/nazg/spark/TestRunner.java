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

public class TestRunner extends WrapperBase implements AutoCloseable {
    private static SparkConf sparkConf = new SparkConf()
            .setAppName("test")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .setMaster("local[*]")
            .set("spark.network.timeout", "10000")
            .set("spark.ui.enabled", "false");

    private final SparkTask testTask;

    public TestRunner(String path) {
        super(new WrapperConfig());

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

        try (InputStream input = getClass().getResourceAsStream(path)) {
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

            testTask = new SparkTask(context);
            testTask.setTaskConfig(wrapperConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, JavaRDDLike> go() throws Exception {
        Map<String, JavaRDDLike> rdds = new HashMap<>();

        HadoopInput hi = new HadoopInput();
        hi.setContext(testTask.context);
        for (String sink : testTask.taskConfig.getInputSink()) {
            WrapperConfig taskConfig = (WrapperConfig) testTask.taskConfig;
            hi.setProperties(sink, taskConfig);
            rdds.put(sink, hi.load(taskConfig.inputPath(sink)));
        }

        processTaskChain(testTask, rdds);

        return rdds;
    }

    public void close() {
        testTask.context.stop();
    }
}
