package ash.nazg.math.functions.keyed;

import java.util.List;

public class SumFunction extends KeyedFunction {
    public SumFunction(Double _const) {
        super(_const);
    }

    @Override
    public Double calcSeries(List<Double> series) {
        double result = (_const != null) ? _const : 0.D;

        for (Double value : series) {
            result += value;
        }

        return result;
    }
}
