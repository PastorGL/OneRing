package ash.nazg.math.functions.keyed;

import java.util.List;

public class MulFunction extends KeyedFunction {
    public MulFunction(Double _const) {
        super(_const);
    }

    @Override
    public Double calcSeries(List<Double> series) {
        double result = (_const != null) ? _const : 1.D;

        for (Double value : series) {
            result *= value;
        }

        return result;
    }
}
