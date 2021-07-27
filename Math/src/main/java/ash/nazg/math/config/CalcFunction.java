/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.config;

import ash.nazg.config.tdl.metadata.DefinitionEnum;

public enum CalcFunction implements DefinitionEnum {
    SUM("Calculate the sum of columns, optionally add a constant"),
    POWERMEAN("Calculate the power mean of columns with a set power"),
    AVERAGE("Calculate the arithmetic mean of columns, optionally shifted towards a constant"),
    RMS("Calculate the square root of the mean square (quadratic mean or RMS)"),
    MIN("Find the minimal value among columns, optionally with a set floor"),
    MAX("Find the maximal value among columns, optionally with a set ceil"),
    MUL("Multiply column values, optionally also by a constant"),
    DIV("Divide first columns by all others, optionally also by a constant"),
    EQUALITY("Check equality of values, optionally within a threshold constant");

    private final String descr;

    CalcFunction(String descr) {
        this.descr = descr;
    }

    @Override
    public String descr() {
        return descr;
    }
}
