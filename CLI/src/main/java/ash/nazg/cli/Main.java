/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.cli;

import ash.nazg.config.TaskWrapperConfigBuilder;
import ash.nazg.config.tdl.Constants;
import ash.nazg.config.tdl.Direction;
import ash.nazg.config.tdl.LayerResolver;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashSet;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        TaskWrapperConfigBuilder configBuilder = new TaskWrapperConfigBuilder();
        configBuilder.addOption("D", "wrapperMetricsPath", true, "Path where to store data stream metrics, if needed");

        JavaSparkContext context = null;
        try {
            configBuilder.setCommandLine(args, "CLI");

            SparkConf sparkConf = new SparkConf()
                    .setAppName("One Ring CLI")
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            boolean local = configBuilder.hasOption("local");
            if (local) {
                String cores = "*";
                if (configBuilder.hasOption("localCores")) {
                    cores = configBuilder.getOptionValue("localCores");
                }

                sparkConf
                        .setMaster("local[" + cores + "]")
                        .set("spark.network.timeout", "10000");

                if (configBuilder.hasOption("driverMemory")) {
                    sparkConf.set("spark.driver.memory", configBuilder.getOptionValue("driverMemory"));
                }
                sparkConf.set("spark.ui.enabled", String.valueOf(configBuilder.hasOption("sparkUI")));
            }

            context = new JavaSparkContext(sparkConf);
            context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

            TaskDefinitionLanguage.Task config = configBuilder.build(context);
            configBuilder.foreignLayerVariable(config, "metrics.store", "D");

            String inputPath = null;
            String outputPath = null;

            if (!local) {
                TaskDefinitionLanguage.Definitions props = config.foreignLayer(Constants.DIST_LAYER);
                LayerResolver distResolver = new LayerResolver(props);

                Direction distDirection = Direction.parse(distResolver.get("wrap", "nop"));
                if (distDirection.toCluster) {
                    TaskDefinitionLanguage.Definitions inputLayer = config.foreignLayer(Constants.INPUT_LAYER);
                    new HashSet<>(inputLayer.keySet()).forEach(k -> inputLayer.compute(k, (key, v) -> {
                        if (key.startsWith(Constants.PATH_PREFIX)) {
                            return null;
                        }
                        return v;
                    }));

                    config.setForeignLayer(Constants.INPUT_LAYER + "." + Constants.PATH, null);
                }
                if (distDirection.fromCluster) {
                    TaskDefinitionLanguage.Definitions outputLayer = config.foreignLayer(Constants.OUTPUT_LAYER);
                    new HashSet<>(outputLayer.keySet()).forEach(k -> outputLayer.compute(k, (key, v) -> {
                        if (key.startsWith(Constants.PATH_PREFIX)) {
                            return null;
                        }
                        return v;
                    }));

                    config.setForeignLayer(Constants.OUTPUT_LAYER + "." + Constants.PATH, null);
                }
            } else {
                inputPath = config.getForeignValue(Constants.INPUT_LAYER + "." + Constants.PATH);
                outputPath = config.getForeignValue(Constants.OUTPUT_LAYER + "." + Constants.PATH);
            }

            if (configBuilder.hasOption("i")) {
                inputPath = configBuilder.getOptionValue("i");
            }
            if (inputPath == null) {
                inputPath = local ? "." : "hdfs:///input";
                config.setForeignLayer(Constants.INPUT_LAYER + "." + Constants.PATH, inputPath);
            }
            if (local && config.getForeignValue(Constants.INPUT_LAYER + "." + Constants.PATH_PREFIX + Constants.DEFAULT_DS) == null) {
                config.setForeignLayer(Constants.INPUT_LAYER + "." + Constants.PATH_PREFIX + Constants.DEFAULT_DS, inputPath);
            }

            if (configBuilder.hasOption("o")) {
                outputPath = configBuilder.getOptionValue("o");
            }
            if (outputPath == null) {
                outputPath = local ? "." : "hdfs:///output";
                config.setForeignLayer(Constants.OUTPUT_LAYER + "." + Constants.PATH, outputPath);
            }
            if (local && config.getForeignValue(Constants.OUTPUT_LAYER + "." + Constants.PATH_PREFIX + Constants.DEFAULT_DS) == null) {
                config.setForeignLayer(Constants.OUTPUT_LAYER + "." + Constants.PATH_PREFIX + Constants.DEFAULT_DS, outputPath);
            }

            new TaskRunner(context, config)
                    .go(local);
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                configBuilder.printHelp("CLI");
            } else {
                LOG.error(ex.getMessage(), ex);
            }

            System.exit(1);
        } finally {
            if (context != null) {
                context.stop();
            }
        }
    }
}
