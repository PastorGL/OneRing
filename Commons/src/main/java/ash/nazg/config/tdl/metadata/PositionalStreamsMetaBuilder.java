package ash.nazg.config.tdl.metadata;

import ash.nazg.config.tdl.StreamType;

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

    public PositionalStreamsMetaBuilder ds(String descr, StreamType[] type, boolean columnar) {
        meta = new PositionalStreamsMeta(positional, descr, type, columnar);

        return this;
    }

    public PositionalStreamsMetaBuilder ds(String descr, StreamType[] type) {
        meta = new PositionalStreamsMeta(positional, descr, type, false);

        return this;
    }

    public PositionalStreamsMetaBuilder genCol(String colName, String colDescr) {
        meta.streams.generated.put(colName, colDescr);

        return this;
    }

    public PositionalStreamsMetaBuilder genCol(Map<String, String> genCol) {
        meta.streams.generated.putAll(genCol);

        return this;
    }

    public PositionalStreamsMeta build() {
        return meta;
    }
}
