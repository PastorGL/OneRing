package ash.nazg.config.tdl.metadata;

import ash.nazg.config.tdl.StreamType;

public class PositionalStreamsMeta extends DataStreamsMeta {
    public final int positional;

    public final DataStreamMeta streams;

    PositionalStreamsMeta(int min, String descr, StreamType[] type, boolean columnar) {
        this.positional = min;

        this.streams = new DataStreamMeta(descr, type, columnar);
    }
}
