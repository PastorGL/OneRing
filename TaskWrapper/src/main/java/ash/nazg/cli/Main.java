package ash.nazg.cli;

import ash.nazg.cli.config.TaskWrapperConfig;
import ash.nazg.cli.config.TaskWrapperConfigBuilder;
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
            configBuilder.addOption("l", "local", false, "Run in local[*] mode");
            configBuilder.addOption("m", "memory", true, "Driver memory for local mode (no effect otherwise)");

            configBuilder.setCommandLine(args);

            SparkConf sparkConf = new SparkConf()
                    .setAppName(WrapperBase.APP_NAME)
                    .set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getCanonicalName());

            if (configBuilder.hasOption("local")) {
                sparkConf
                        .setMaster("local[*]")
                        .set("spark.network.timeout", "10000");

                if (configBuilder.hasOption("driverMemory")) {
                    sparkConf
                            .set("spark.driver.memory", configBuilder.getOptionValue("driverMemory"));
                }
            }

            context = new JavaSparkContext(sparkConf);
            context.hadoopConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, Boolean.TRUE.toString());

            TaskWrapperConfig config = configBuilder.build(context);
            configBuilder.overrideFromCommandLine(DS_OUTPUT_PATH, "o");
            configBuilder.overrideFromCommandLine(TaskWrapperConfig.META_DISTCP_STORE_PATH, "S");

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
