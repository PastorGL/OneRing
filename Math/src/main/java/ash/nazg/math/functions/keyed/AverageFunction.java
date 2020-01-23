package ash.nazg.math.functions.keyed;

import java.util.List;

public class AverageFunction extends KeyedFunction {
    public AverageFunction(Double shift) {
        super(shift);
    }

    @Override
    public Double calcSeries(List<Double> series) {
        double result = (_const == null) ? 0.D : _const;

        for (Double value : series) {
            result += value;
        }

        return result / series.size();
    }
}
