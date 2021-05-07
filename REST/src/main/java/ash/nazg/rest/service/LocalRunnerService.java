/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.cli.TaskRunner;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import javax.inject.Inject;
import javax.inject.Singleton;
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
                                                    .setAppName("One Ring REST Local Runner")
                                                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName())
                                                    .set("spark.network.timeout", "10000");

                                            if (props.containsKey("local.driver.memory")) {
                                                sparkConf
                                                        .set("spark.driver.memory", props.getProperty("local.driver.memory"));
                                            }

                                            context = new JavaSparkContext(sparkConf);
                                            context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

                                            new TaskRunner(context, current.task)
                                                    .go(true);

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
    public String define(TaskDefinitionLanguage.Task task) {
        int taskId = queue.size();
        queue.put(taskId, new QueueEntry(task));

        return String.valueOf(taskId);
    }

    private static class QueueEntry {
        private final TaskDefinitionLanguage.Task task;
        private TaskStatus status = TaskStatus.QUEUED;

        private QueueEntry(TaskDefinitionLanguage.Task task) {
            this.task = task;
        }
    }
}
