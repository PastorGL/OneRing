/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.spark.Operation;

public abstract class PopulationIndicatorOperation extends Operation {
    protected int countColumn;
    protected int valueColumn;

    protected String inputValuesName;
    protected char inputValuesDelimiter;

    protected String outputName;
    protected char outputDelimiter;

    @Override
    public void configure() throws InvalidConfigValueException {
        outputName = opResolver.positionalOutput(0);
        outputDelimiter = dsResolver.outputDelimiter(outputName);
    }
}
