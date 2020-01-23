package ash.nazg.math.functions.keyed;

import java.util.List;

public class MinFunction extends KeyedFunction {
    public MinFunction(Double floor) {
        super(floor);
    }

    @Override
    public Double calcSeries(List<Double> series) {
        double result = Double.POSITIVE_INFINITY;

        for (Double value : series) {
            result = Math.min(result, value);
        }
        if ((_const != null) && (_const > result)) {
            result = _const;
        }

        return result;
    }
}
