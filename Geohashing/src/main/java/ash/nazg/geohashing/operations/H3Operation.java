/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.geohashing.operations;

import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.geohashing.functions.H3Function;
import ash.nazg.geohashing.functions.HasherFunction;

@SuppressWarnings("unused")
public class H3Operation extends GeohashingOperation {
    private static final Integer DEF_HASH_LEVEL = 9;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("h3", "For each input row with a coordinate pair, generate" +
                " Uber H3 hash with a selected level",

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD with coordinates",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_LAT_COLUMN, "Column with latitude, degrees")
                        .def(DS_LON_COLUMN, "Column with longitude, degrees")
                        .def(OP_HASH_LEVEL, "Level of the hash", Integer.class,
                                getDefaultLevel() + "", "Default hash level")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD with coordinates' H3 hash",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .genCol(GEN_HASH, "Column with a generated H3 hash hexadecimal string")
                        .build()
        );
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
