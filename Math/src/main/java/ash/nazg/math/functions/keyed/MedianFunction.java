/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.keyed;

import java.util.Collections;
import java.util.List;

public class MedianFunction extends KeyedFunction {
    public MedianFunction() {
        super(null);
    }

    @Override
    public Double calcSeries(List<Double> series) {
        Collections.sort(series);

        int size = series.size();
        int m = size >> 1;
        return (size % 2 == 0)
                ? (series.get(m) + series.get(m - 1)) / 2.D
                : series.get(m);
    }
}
