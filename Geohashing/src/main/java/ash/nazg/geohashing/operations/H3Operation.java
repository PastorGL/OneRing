/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.geohashing.operations;

import ash.nazg.config.tdl.Description;
import ash.nazg.geohashing.functions.H3Function;
import ash.nazg.geohashing.functions.HasherFunction;

@SuppressWarnings("unused")
public class H3Operation extends GeohashingOperation{
    @Description("Default hash level")
    public static final Integer DEF_HASH_LEVEL = 9;

    public static final String VERB = "h3";

    @Override
    @Description("For each input row with a coordinate pair, generate Uber H3 hash with a selected level")
    public String verb() {
        return VERB;
    }

    @Override
    protected int getMinLevel() {
        return 0;
    }

    @Override
    protected int getMaxLevel() {
        return 15;
    }

    @Override
    protected Integer getDefaultLevel() {
        return DEF_HASH_LEVEL;
    }

    @Override
    protected HasherFunction getHasher() {
        return new H3Function(level);
    }
}
