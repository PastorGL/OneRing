/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.config;

import ash.nazg.math.functions.series.NormalizeFunction;
import ash.nazg.math.functions.series.SeriesFunction;
import ash.nazg.math.functions.series.StdDevFunction;
import ash.nazg.math.operations.SeriesMathOperation;
import ash.nazg.metadata.DefinitionEnum;

public enum SeriesMath implements DefinitionEnum {
    STDDEV("Calculate Standard Deviation of a value", StdDevFunction.class),
    NORMALIZE("Re-normalize value into a range of 0.." + SeriesMathOperation.CALC_CONST, NormalizeFunction.class);

    private final String descr;
    private final Class<? extends SeriesFunction> function;

    SeriesMath(String descr, Class<? extends SeriesFunction> function) {
        this.descr = descr;
        this.function = function;
    }

    @Override
    public String descr() {
        return descr;
    }

    public SeriesFunction function(String column, Double _const) throws Exception {
        return function.getConstructor(String.class, Double.class).newInstance(column, _const);
    }
}
