package ash.nazg.config.tdl.metadata;

import java.util.HashMap;
import java.util.Map;

public class NamedStreamsMeta extends DataStreamsMeta {
    public final Map<String, DataStreamMeta> streams;

    NamedStreamsMeta() {
        this.streams = new HashMap<>();
    }
}
