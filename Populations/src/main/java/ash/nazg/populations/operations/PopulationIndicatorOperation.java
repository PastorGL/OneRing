/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.spark.Operation;

import java.util.Properties;

public abstract class PopulationIndicatorOperation extends Operation {
    protected int countColumn;
    protected int valueColumn;

    protected String inputValuesName;
    protected char inputValuesDelimiter;

    protected String outputName;
    protected char outputDelimiter;

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        outputName = describedProps.outputs.get(0);
        outputDelimiter = dataStreamsProps.outputDelimiter(outputName);
    }
}
