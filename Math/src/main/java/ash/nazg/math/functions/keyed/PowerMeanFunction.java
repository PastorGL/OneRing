package ash.nazg.math.functions.keyed;

import java.util.List;

public class PowerMeanFunction extends KeyedFunction {
    public PowerMeanFunction(double pow) {
        super(pow);
    }

    @Override
    public Double calcSeries(List<Double> series) {
        double result = 0.D;

        for (Double value: series) {
            result += Math.pow(value, _const);
        }

        return Math.pow(result / series.size(), 1.D / _const);
    }
}
