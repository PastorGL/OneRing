package ash.nazg.storage;

import java.util.regex.Pattern;

public abstract class HadoopAdapter implements StorageAdapter {
    @Override
    public boolean isFallback() {
        return true;
    }

    public Pattern proto() {
        return Adapters.PATH_PATTERN;
    }
}
