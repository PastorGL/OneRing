/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.geohashing.operations;

import ash.nazg.config.tdl.Description;
import ash.nazg.geohashing.functions.HasherFunction;
import ash.nazg.geohashing.functions.JapanMeshFunction;

@SuppressWarnings("unused")
public class JapanMeshOperation extends GeohashingOperation {
    @Description("Default hash level")
    public static final Integer DEF_HASH_LEVEL = 6;

    public static final String VERB = "japanMesh";

    @Override
    @Description("For each input row with a coordinate pair, generate Japan Mesh hash with a selected level")
    public String verb() {
        return VERB;
    }

    @Override
    protected int getMinLevel() {
        return 1;
    }

    @Override
    protected int getMaxLevel() {
        return 6;
    }

    @Override
    protected Integer getDefaultLevel() {
        return DEF_HASH_LEVEL;
    }

    @Override
    protected HasherFunction getHasher() {
        return new JapanMeshFunction(level);
    }
}
