/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.tdl.Description;

import java.util.regex.Pattern;

public abstract class S3DirectAdapter implements StorageAdapter {
    protected static final Pattern PATTERN = Pattern.compile("^s3d://([^/]+)/(.+)");

    protected char delimiter;

    @Description("S3 Direct adapter for any S3-compatible storage")
    public Pattern proto() {
        return PATTERN;
    }
}
