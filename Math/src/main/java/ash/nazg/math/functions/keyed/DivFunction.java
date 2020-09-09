/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.keyed;

import java.util.List;

public class DivFunction extends KeyedFunction {
    public DivFunction(Double _const) {
        super(_const);
    }

    @Override
    public Double calcSeries(List<Double> series) {
        double result = series.remove(0);

        for (Double value : series) {
            result /= value;
        }

        return (_const != null) ? (result / _const) : result;
    }
}
