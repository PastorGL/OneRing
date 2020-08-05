/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.cli;

import ash.nazg.config.TaskWrapperConfigBuilder;
import ash.nazg.config.WrapperConfig;
import ash.nazg.spark.WrapperBase;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import static ash.nazg.config.WrapperConfig.DS_OUTPUT_PATH;

public class Main {
    private static final Logger LOG = Logger.getLogger(Main.class);

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        TaskWrapperConfigBuilder configBuilder = new TaskWrapperConfigBuilder();
        JavaSparkContext context = null;

        try {
            configBuilder.addOption("c", "config", true, "Config file (JSON or .ini format)");
            configBuilder.addOption("o", "output", true, "Output path");

            configBuilder.setCommandLine(args);

            SparkConf sparkConf = new SparkConf()
                    .setAppName(WrapperBase.APP_NAME)
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            if (configBuilder.hasOption("local")) {
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

            WrapperConfig config = configBuilder.build(context);
            configBuilder.overrideFromCommandLine(DS_OUTPUT_PATH, "o");
            configBuilder.overrideFromCommandLine("distcp.store", "S");

            new TaskWrapper(context, config)
                    .go();
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                new HelpFormatter().printHelp(WrapperBase.APP_NAME, configBuilder.getOptions());
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
