/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.cli.config;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.PropertiesConfig;
import ash.nazg.storage.Adapters;
import ash.nazg.config.WrapperConfig;
import ash.nazg.config.tdl.PropertiesConverter;
import org.apache.commons.cli.*;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class WrapperConfigBuilder {
    protected WrapperConfig wrapperConfig;
    protected Options options;
    protected CommandLine commandLine;

    public void addOption(String opt, String longOpt, boolean hasArg, String description) {
        options.addOption(opt, longOpt, hasArg, description);
    }

    public void addRequiredOption(String opt, String longOpt, boolean hasArg, String description) {
        Option option = new Option(opt, longOpt, hasArg, description);
        option.setRequired(true);
        options.addOption(option);
    }

    /**
     * Get command line option value.
     *
     * @param opt either short or long option
     * @return null, if wasn't set
     */
    public String getOptionValue(String opt) {
        return commandLine.getOptionValue(opt);
    }

    public boolean hasOption(String opt) {
        return commandLine.hasOption(opt);
    }

    /**
     * Parse the command line skipping all unknown (not explicitly added) command line arguments and extract task prefix
     *
     * @param args array of command line arguments
     * @return this task's {@link PropertiesConfig} instance
     * @throws ParseException if any of the known options value is bad
     */
    public void setCommandLine(String[] args) throws ParseException {
        commandLine = new BasicParser() {
            @Override
            protected void processOption(String arg, ListIterator iter) throws ParseException {
                if (getOptions().hasOption(arg)) {
                    super.processOption(arg, iter);
                }
            }
        }.parse(options, args);
    }

    protected Properties loadConfig(String source, JavaSparkContext context, String prefix) {
        final Properties ini = new Properties();

        Map fromCtx = null;
        if ((context != null) && (prefix != null)) {
            Stream<Tuple2<String, String>> stream = Arrays.stream(context.getConf().getAll());

            fromCtx = stream
                    .filter(t -> t._1.startsWith(prefix))
                    .collect(Collectors.toMap(t -> t._1, t -> t._2));
        }

        if (fromCtx != null) {
            ini.putAll(fromCtx);
        }

        if (source != null) {
            List<String> l;
            if (Adapters.PATH_PATTERN.matcher(source).matches() && (context != null)) {
                l = context.wholeTextFiles(new File(source).getParent())
                        .filter(t -> t._1.equals(source))
                        .flatMap(t -> {
                            String[] s = t._2.split("\\R+");
                            return Arrays.asList(s).iterator();
                        })
                        .collect();
            } else {
                try {
                    l = Files.readAllLines(Paths.get(source));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            boolean convert = source.endsWith(".json");
            if (convert) {
                try {
                    ini.putAll(PropertiesConverter.toProperties(String.join("", l)));
                } catch (Exception e) {
                    throw new InvalidConfigValueException("Invalid JSON config", e);
                }
            } else {
                ini.putAll(l.stream()
                        .filter(t -> (prefix != null) ? t.startsWith(prefix) : !t.isEmpty())
                        .map(t -> t.split("=", 2))
                        .collect(Collectors.toMap(t -> t[0], t -> t[1]))
                );
            }
        }

        if (ini.isEmpty()) {
            throw new InvalidConfigValueException("Configuration source '" + source + "' is empty and Spark context doesn't have properties for a task '" + prefix + "'");
        }

        return ini;
    }

    public Options getOptions() {
        return options;
    }
}
