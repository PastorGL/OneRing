/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.WrapperConfig;

import java.util.regex.Pattern;

public interface StorageAdapter {
    Pattern proto();

    void setProperties(String name, WrapperConfig wrapperConfig);
}
