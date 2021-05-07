package ash.nazg.storage;

import java.util.regex.Pattern;

public class AdapterInfo {
    public final Pattern proto;
    public final Class<? extends StorageAdapter> adapterClass;

    public AdapterInfo(Pattern proto, Class<? extends StorageAdapter> adapterClass) {
        this.proto = proto;
        this.adapterClass = adapterClass;
    }
}
