/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.series;

import org.apache.spark.api.java.JavaDoubleRDD;

public class StdDevFunction extends SeriesFunction {
    private double stdDev;
    private double mean;

    public StdDevFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int columnForCalculation) {
        super(inputDelimiter, outputDelimiter, outputColumns, columnForCalculation);
    }

    @Override
    public void calcSeries(JavaDoubleRDD series) {
        if (series.count() < 30) {
            stdDev = series.stdev();
        } else {
            stdDev = series.sampleStdev();
        }
        mean = series.mean();
    }

    @Override
    public String[] calcLine(String[] row) {
        String[] out = new String[outputColumns.length];

        for (int i = 0; i < outputColumns.length; i++) {
            if (outputColumns[i] > 0) {
                out[i] = row[outputColumns[i]];
            } else {
                double result = (new Double(row[columnForCalculation]) - mean) / stdDev;
                out[i] = Double.toString(result);
            }
        }

        return out;
    }
}
