/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.columns;

import ash.nazg.math.functions.MathFunction;

public abstract class ColumnsMathFunction extends MathFunction {
    protected final int[] columnsForCalculation;
    protected final Double _const;

    public ColumnsMathFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int[] columnsForCalculation, Double _const) {
        super(inputDelimiter, outputDelimiter, outputColumns);
        this.columnsForCalculation = columnsForCalculation;
        this._const = _const;
    }
}
