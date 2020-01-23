package ash.nazg.rest.service;

import ash.nazg.config.tdl.PropertiesConverter;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import ash.nazg.cli.TaskWrapper;
import ash.nazg.cli.config.TaskWrapperConfig;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.StringReader;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class LocalRunnerService extends Runner {
    static private AtomicInteger last = new AtomicInteger();
    static private Map<Integer, QueueEntry> queue = new ConcurrentSkipListMap<>();

    private static Properties props;

    static {
        new Thread(() -> {
            while (true) {
                try {
                    QueueEntry lastEntry = queue.get(last.get());

                    if (lastEntry != null) {
                        switch (lastEntry.status) {
                            case SUCCESS:
                            case FAILURE:
                                lastEntry = queue.get(last.incrementAndGet());
                            case QUEUED: {
                                if (lastEntry != null) {
                                    lastEntry.status = TaskStatus.RUNNING;

                                    final QueueEntry current = lastEntry;
                                    new Thread(() -> {
                                        JavaSparkContext context = null;

                                        try {
                                            SparkConf sparkConf = new SparkConf()
                                                    .setMaster("local[*]")
                                                    .setAppName("LocalRunner")
                                                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                                    .set("spark.network.timeout", "10000");

                                            if (props.containsKey("local.driver.memory")) {
                                                sparkConf
                                                        .set("spark.driver.memory", props.getProperty("local.driver.memory"));
                                            }

                                            TaskWrapperConfig config = new LocalRunnerConfigBuilder()
                                                    .build(current);
                                            context = new JavaSparkContext(sparkConf);
                                            context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

                                            new TaskWrapper(context, config)
                                                    .go();

                                            current.status = TaskStatus.SUCCESS;
                                        } catch (Exception e) {
                                            current.status = TaskStatus.FAILURE;
                                        } finally {
                                            if (context != null) {
                                                context.stop();
                                            }
                                        }
                                    }, "one-ring-local-task-" + last.get()).start();
                                }

                                break;
                            }
                        }
                    }

                    Thread.sleep(5000);
                } catch (Exception ignore) {
                }
            }
        }, "one-ring-local-task-queue-processor").start();
    }

    @Inject
    public LocalRunnerService(Properties props) {
        LocalRunnerService.props = props;
    }

    @Override
    public TaskStatus status(String taskId) {
        QueueEntry entry = queue.get(Integer.valueOf(taskId));

        return (entry == null)
                ? TaskStatus.NOT_FOUND
                : entry.status;
    }

    @Override
    public String define(TaskDefinitionLanguage.Task task, String params) {
        int taskId = queue.size();
        queue.put(taskId, new QueueEntry(task, params));

        return String.valueOf(taskId);
    }

    private static class LocalRunnerConfigBuilder {
        private TaskWrapperConfig build(QueueEntry entry) {
            Properties overrides = new Properties();

            if (entry.variables != null) {
                String variables = new String(Base64.getDecoder().decode(entry.variables));

                try {
                    overrides.load(new StringReader(variables));
                } catch (IOException ignored) {
                }
            }

            Properties props = PropertiesConverter.toProperties(entry.task);

            TaskWrapperConfig wrapperConfig = new TaskWrapperConfig();
            wrapperConfig.setPrefix(entry.task.prefix);
            wrapperConfig.setOverrides(overrides);
            wrapperConfig.setProperties(props);

            return wrapperConfig;
        }
    }

    private static class QueueEntry {
        private final TaskDefinitionLanguage.Task task;
        private final String variables;
        private TaskStatus status = TaskStatus.QUEUED;

        private QueueEntry(TaskDefinitionLanguage.Task task, String variables) {
            this.task = task;
            this.variables = variables;
        }
    }
}