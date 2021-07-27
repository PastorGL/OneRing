package ash.nazg.config.tdl.metadata;

import ash.nazg.config.tdl.StreamType;

public class NamedStreamsMetaBuilder {
    private final NamedStreamsMeta meta;

    public NamedStreamsMetaBuilder() {
        this.meta = new NamedStreamsMeta();
    }

    public NamedStreamsMetaBuilder ds(String name, String descr, StreamType[] type, boolean columnar) {
        meta.streams.put(name, new DataStreamMeta(descr, type, columnar));

        return this;
    }

    public NamedStreamsMetaBuilder ds(String name, String descr, StreamType[] type) {
        meta.streams.put(name, new DataStreamMeta(descr, type, false));

        return this;
    }

    public NamedStreamsMetaBuilder genCol(String name, String colName, String colDescr) {
        meta.streams.get(name).generated.put(colName, colDescr);

        return this;
    }

    public NamedStreamsMeta build() {
        return meta;
    }
}
