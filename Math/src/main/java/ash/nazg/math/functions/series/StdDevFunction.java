/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.series;

import ash.nazg.data.Record;
import org.apache.spark.api.java.JavaDoubleRDD;

import java.util.List;

public class StdDevFunction extends SeriesFunction {
    private double stdDev;
    private double mean;

    public StdDevFunction(String calcProp, Double ignore) {
        super(calcProp, ignore);
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
    public Double calcValue(Record row) {
        return (row.asDouble(calcProp) - mean) / stdDev;
    }
}
