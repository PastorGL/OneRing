/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.tdl.Description;

import java.util.regex.Pattern;

public abstract class HadoopAdapter implements StorageAdapter {
    @Description("Default Storage that utilizes Hadoop filesystems")
    public Pattern proto() {
        return Adapters.PATH_PATTERN;
    }
}
