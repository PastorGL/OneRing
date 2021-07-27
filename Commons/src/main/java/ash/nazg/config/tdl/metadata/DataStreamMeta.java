package ash.nazg.config.tdl.metadata;

import ash.nazg.config.tdl.StreamType;

import java.util.HashMap;
import java.util.Map;

public class DataStreamMeta {
    public final String descr;

    public final StreamType[] type;

    public final Map<String, String> generated;
    public final boolean columnar;

    DataStreamMeta(String descr, StreamType[] type, boolean columnar) {
        this.descr = descr;

        this.type = type;

        this.generated = columnar ? new HashMap<>() : null;
        this.columnar = columnar;
    }
}
