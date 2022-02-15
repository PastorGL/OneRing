/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.series;

import org.apache.spark.api.java.JavaDoubleRDD;

public class NormalizeFunction extends SeriesFunction {
    private final double upper;
    private double maxValue;

    public NormalizeFunction(char inputDelimiter, char outputDelimiter, int[] outColumns, Integer calcColumn, double upper) {
        super(inputDelimiter, outputDelimiter, outColumns, calcColumn);
        this.upper = upper;
    }

    @Override
    public void calcSeries(JavaDoubleRDD series) {
        maxValue = series.max();
    }

    @Override
    public String[] calcLine(String[] row) {
        String[] out = new String[outputColumns.length];

        for (int i = 0; i < outputColumns.length; i++) {
            if (outputColumns[i] >= 0) {
                out[i] = row[outputColumns[i]];
            } else {
                double result = Double.parseDouble(row[columnForCalculation]) / maxValue * upper;
                out[i] = Double.toString(result);
            }
        }

        return out;
    }
}
