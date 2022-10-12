/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.series;

import ash.nazg.data.Record;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static ash.nazg.math.operations.SeriesMathOperation.GEN_RESULT;

public abstract class SeriesFunction implements FlatMapFunction<Iterator<Object>, Object> {
    protected final String calcProp;
    protected final Double _const;

    public SeriesFunction(String calcProp, Double _const) {
        this.calcProp = calcProp;
        this._const = _const;
    }

    public abstract void calcSeries(JavaDoubleRDD series);

    public abstract Double calcValue(Record row);

    @Override
    final public Iterator<Object> call(Iterator<Object> it) {
        List<Object> ret = new ArrayList<>();

        while (it.hasNext()) {
            Record row = (Record) it.next();

            Record rec = (Record) row.clone();
            rec.put(GEN_RESULT, calcValue(rec));

            ret.add(rec);
        }

        return ret.iterator();
    }
}
