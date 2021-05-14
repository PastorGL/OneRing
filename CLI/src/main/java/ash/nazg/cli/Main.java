/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.cli;

import ash.nazg.config.TaskWrapperConfigBuilder;
import ash.nazg.config.tdl.Constants;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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
            configBuilder.foreignLayerVariable(config, "dist.store", "S");
            configBuilder.foreignLayerVariable(config, "metrics.store", "D");

            TaskDefinitionLanguage.DataStream defaultDs = config.dataStreams.get(Constants.DEFAULT_DS);
            if (configBuilder.hasOption("i")) {
                defaultDs.input.path = configBuilder.getOptionValue("i");
            } else if (StringUtils.isEmpty(defaultDs.input.path)) {
                defaultDs.input.path = local ? "." : "hdfs:///input";
            }
            if (configBuilder.hasOption("o")) {
                defaultDs.output.path = configBuilder.getOptionValue("o");
            } else if (StringUtils.isEmpty(defaultDs.output.path)) {
                defaultDs.output.path = local ? "." : "hdfs:///output";
            }
            if (!local) {
                for (String dsName : config.dataStreams.keySet()) {
                    if (!Constants.DEFAULT_DS.equals(dsName)) {
                        TaskDefinitionLanguage.DataStream ds = config.dataStreams.get(dsName);
                        if (ds.input != null) {
                            ds.input.path = null;
                        }
                        if (ds.output != null) {
                            ds.output.path = null;
                        }
                    }
                }
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
