/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import org.apache.commons.cli.*;

import java.util.ListIterator;

public abstract class WrapperConfigBuilder {
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
     * @throws ParseException if any of the known options value is bad
     */
    public void setCommandLine(String[] args, String utility) throws ParseException {
        commandLine = new BasicParser() {
            @Override
            protected void processOption(String arg, ListIterator iter) throws ParseException {
                if (getOptions().hasOption(arg)) {
                    super.processOption(arg, iter);
                }
            }
        }.parse(options, args);

        if (commandLine.hasOption("help")) {
            printHelp(utility);

            System.exit(0);
        }
    }

    public void printHelp(String utility) {
        new HelpFormatter().printHelp("One Ring " + utility, options);
    }

    public Options getOptions() {
        return options;
    }
}
