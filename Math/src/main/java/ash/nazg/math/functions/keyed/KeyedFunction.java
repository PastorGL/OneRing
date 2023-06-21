/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.keyed;

import java.io.Serializable;
import java.util.List;

public abstract class KeyedFunction implements Serializable {
    protected final Double _const;

    protected KeyedFunction(Double _const) {
        this._const = _const;
    }

    public abstract Double calcSeries(List<Double[]> series, int idx);
}
