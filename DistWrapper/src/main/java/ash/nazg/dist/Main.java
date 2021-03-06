/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import ash.nazg.config.TaskWrapperConfigBuilder;
import ash.nazg.config.WrapperConfig;
import ash.nazg.spark.WrapperBase;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {
        TaskWrapperConfigBuilder configBuilder = new TaskWrapperConfigBuilder();
        JavaSparkContext context = null;

        try {
            configBuilder.addRequiredOption("c", "config", true, "Config file");
            configBuilder.addOption("d", "direction", true, "Copy direction. Can be 'from', 'to', or 'nop' to just validate the config file and exit");;

            configBuilder.setCommandLine(args);

            SparkConf sparkConf = new SparkConf()
                    .setAppName(WrapperBase.APP_NAME)
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

            WrapperConfig config = configBuilder.build(context);
            configBuilder.overrideFromCommandLine("distcp.wrap", "d");
            configBuilder.overrideFromCommandLine("distcp.store", "S");

            new DistWrapper(context, config, local)
                    .go();
        } catch (Exception e) {
            e.printStackTrace(System.err);

            System.exit(1);
        } finally {
            if (context != null) {
                context.stop();
            }
        }
    }
}
