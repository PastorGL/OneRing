package ash.nazg.math.config;

import ash.nazg.config.tdl.Description;

public enum CalcFunction {
    @Description("Calculate the sum of columns, optionally add a constant")
    SUM,
    @Description("Calculate the power mean of columns with a set power")
    POWERMEAN, @Description("Alias of POWERMEAN") POWER_MEAN,
    @Description("Calculate the arithmetic mean of columns, optionally shifted towards a constant")
    MEAN, @Description("Alias of MEAN") AVERAGE,
    @Description("Calculate the square root of the mean square (quadratic mean or RMS)")
    RMS, @Description("Alias of RMS") ROOTMEAN, @Description("Alias of RMS") ROOT_MEAN, @Description("Alias of RMS") ROOTMEANSQUARE, @Description("Alias of RMS") ROOT_MEAN_SQUARE,
    @Description("Find the minimal value among columns, optionally with a set floor")
    MIN,
    @Description("Find the maximal value among columns, optionally with a set ceil")
    MAX,
    @Description("Multiply column values, optionally also by a constant")
    MUL, @Description("Alias of MUL") MULTIPLY,
}
