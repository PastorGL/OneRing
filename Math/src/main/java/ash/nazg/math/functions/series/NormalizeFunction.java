/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.series;

import ash.nazg.data.Record;
import org.apache.spark.api.java.JavaDoubleRDD;

import java.util.List;

public class NormalizeFunction extends SeriesFunction {
    private double maxValue;

    public NormalizeFunction(String calcProp, Double upper) {
        super(calcProp, upper);
    }

    @Override
    public void calcSeries(JavaDoubleRDD series) {
        maxValue = series.max();
    }

    @Override
    public Double calcValue(Record row) {
        return row.asDouble(calcProp) / maxValue * _const;
    }
}
