/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.series;

import ash.nazg.math.functions.MathFunction;
import org.apache.spark.api.java.JavaDoubleRDD;

public abstract class SeriesFunction extends MathFunction {
    protected final int columnForCalculation;

    public SeriesFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int columnForCalculation) {
        super(inputDelimiter, outputDelimiter, outputColumns);
        this.columnForCalculation = columnForCalculation;
    }

    public abstract void calcSeries(JavaDoubleRDD series);
}
