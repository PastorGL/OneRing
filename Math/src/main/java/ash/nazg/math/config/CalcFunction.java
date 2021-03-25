/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.config;

import ash.nazg.config.tdl.Description;

public enum CalcFunction {
    @Description("Calculate the sum of columns, optionally add a constant")
    SUM,
    @Description("Calculate the power mean of columns with a set power")
    POWERMEAN,
    @Description("Calculate the arithmetic mean of columns, optionally shifted towards a constant")
    AVERAGE,
    @Description("Calculate the square root of the mean square (quadratic mean or RMS)")
    RMS,
    @Description("Find the minimal value among columns, optionally with a set floor")
    MIN,
    @Description("Find the maximal value among columns, optionally with a set ceil")
    MAX,
    @Description("Multiply column values, optionally also by a constant")
    MUL,
    @Description("Divide first columns by all others, optionally also by a constant")
    DIV,
}
