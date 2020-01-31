/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import java.util.regex.Pattern;

public abstract class S3DirectAdapter implements StorageAdapter {
    protected static final Pattern PATTERN = Pattern.compile("^s3d://([^/]+)/(.+)");

    protected char delimiter;

    public Pattern proto() {
        return PATTERN;
    }
}
