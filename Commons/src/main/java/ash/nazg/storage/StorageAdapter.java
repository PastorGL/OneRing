package ash.nazg.storage;

import ash.nazg.config.WrapperConfig;

import java.util.regex.Pattern;

public interface StorageAdapter {
    Pattern proto();

    default boolean isFallback() {
        return false;
    }

    void setProperties(String name, WrapperConfig wrapperConfig);
}
