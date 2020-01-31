/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
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
