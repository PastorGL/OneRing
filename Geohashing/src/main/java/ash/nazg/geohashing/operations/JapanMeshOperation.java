/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.geohashing.operations;

import ash.nazg.metadata.DefinitionMetaBuilder;
import ash.nazg.metadata.OperationMeta;
import ash.nazg.metadata.Origin;
import ash.nazg.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.data.StreamType;
import ash.nazg.geohashing.functions.HasherFunction;
import ash.nazg.geohashing.functions.JapanMeshFunction;

@SuppressWarnings("unused")
public class JapanMeshOperation extends GeohashingOperation {
    private static final Integer DEF_HASH_LEVEL = 6;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("japanMesh", "For each input row with a coordinate pair, generate" +
                " Japan Mesh hash with a selected level",

                new PositionalStreamsMetaBuilder()
                        .input("Columnar RDD with coordinates",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(LAT_COLUMN, "For a Columnar DataStream only, column with latitude, degrees",
                                DEF_CENTER_LAT, "By default, '" + DEF_CENTER_LAT + "'")
                        .def(LON_COLUMN, "For a Columnar DataStream only, column with longitude, degrees",
                                DEF_CENTER_LON, "By default, '" + DEF_CENTER_LON + "'")
                        .def(HASH_LEVEL, "Level of the hash", Integer.class,
                                getDefaultLevel() + "", "Default hash level")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .output("DataStream with hashed with coordinates",
                                new StreamType[]{StreamType.Columnar, StreamType.Point}, Origin.AUGMENTED, null
                        )
                        .generated(GEN_HASH, "Column with a generated Japan mesh string")
                        .build()
        );
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
