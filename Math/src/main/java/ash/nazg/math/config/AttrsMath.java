/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.config;

import ash.nazg.math.functions.attrs.AttrsFunction;
import ash.nazg.math.functions.attrs.MaxFunction;
import ash.nazg.math.functions.attrs.MedianFunction;
import ash.nazg.math.functions.attrs.MinFunction;
import ash.nazg.metadata.DefinitionEnum;

public enum AttrsMath implements DefinitionEnum {
    MIN("Find the minimal value among attributes, optionally with a set floor", MinFunction.class),
    MAX("Find the maximal value among attributes, optionally with a set ceil", MaxFunction.class),
    MEDIAN("Calculate the median", MedianFunction.class);

    private final String descr;
    private final Class<? extends AttrsFunction> function;

    AttrsMath(String descr, Class<? extends AttrsFunction> function) {
        this.descr = descr;
        this.function = function;
    }

    @Override
    public String descr() {
        return descr;
    }

    public AttrsFunction function(String[] sourceColumns) throws Exception {
        return function.getConstructor(String[].class).newInstance(new Object[]{sourceColumns});
    }
}
