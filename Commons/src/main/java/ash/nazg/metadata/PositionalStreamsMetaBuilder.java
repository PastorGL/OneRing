/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.metadata;

import ash.nazg.data.StreamType;

import java.util.List;
import java.util.Map;

public class PositionalStreamsMetaBuilder {
    private final int positional;

    private PositionalStreamsMeta meta;

    public PositionalStreamsMetaBuilder() {
        positional = 1;
    }

    public PositionalStreamsMetaBuilder(int min) {
        positional = min;
    }

    public PositionalStreamsMetaBuilder input(String descr, StreamType[] type) {
        meta = new PositionalStreamsMeta(positional, descr, type);

        return this;
    }

    public PositionalStreamsMetaBuilder output(String descr, StreamType[] type, Origin origin, List<String> ancestors) {
        meta = new PositionalStreamsMeta(positional, descr, type, origin, ancestors);

        return this;
    }

    public PositionalStreamsMetaBuilder generated(String propName, String propDescr) {
        meta.streams.generated.put(propName, propDescr);

        return this;
    }

    public PositionalStreamsMetaBuilder generated(Map<String, String> genProps) {
        meta.streams.generated.putAll(genProps);

        return this;
    }

    public PositionalStreamsMeta build() {
        return meta;
    }
}
