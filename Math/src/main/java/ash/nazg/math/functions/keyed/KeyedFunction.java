package ash.nazg.math.functions.keyed;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class KeyedFunction implements PairFlatMapFunction<Iterator<Tuple2<Object, List<Double>>>, Object, Double> {
    protected final Double _const;

    public KeyedFunction(Double _const) {
        this._const = _const;
    }

    public abstract Double calcSeries(List<Double> series);

    @Override
    public Iterator<Tuple2<Object, Double>> call(Iterator<Tuple2<Object, List<Double>>> it) {
        List<Tuple2<Object, Double>> ret = new ArrayList<>();

        while (it.hasNext()) {
            Tuple2<Object, List<Double>> t = it.next();

            ret.add(new Tuple2<>(t._1, calcSeries(t._2)));
        }

        return ret.iterator();
    }
}
