package ash.nazg.math.functions.keyed;

import java.util.List;

public class MaxFunction extends KeyedFunction {
    public MaxFunction(Double ceil) {
        super(ceil);
    }

    @Override
    public Double calcSeries(List<Double> series) {
        double result = Double.NEGATIVE_INFINITY;

        for (Double value : series) {
            result = Math.max(result, value);
        }
        if ((_const != null) && (_const < result)) {
            result = _const;
        }

        return result;
    }
}
