package ash.nazg.storage;

import ash.nazg.storage.metadata.AdapterMeta;

public class AdapterInfo {
    public final Class<? extends StorageAdapter> adapterClass;
    public final AdapterMeta meta;

    public AdapterInfo(Class<? extends StorageAdapter> adapterClass, AdapterMeta meta) {
        this.adapterClass = adapterClass;
        this.meta = meta;
    }
}
