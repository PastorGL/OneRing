/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.keyed;

import java.util.List;

public class MulFunction extends KeyedFunction {
    public MulFunction(Double seed) {
        super(seed);
    }

    @Override
    public Double calcSeries(List<Double[]> series, int idx) {
        double result = (_const != null) ? _const : 1.D;

        for (Double[] value : series) {
            result *= value[idx];
        }

        return result;
    }
}
