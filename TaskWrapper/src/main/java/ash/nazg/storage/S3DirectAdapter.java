package ash.nazg.storage;

import java.util.regex.Pattern;

public abstract class S3DirectAdapter implements StorageAdapter {
    protected static final Pattern PATTERN = Pattern.compile("^s3d://([^/]+)/(.+)");

    protected char delimiter;

    public Pattern proto() {
        return PATTERN;
    }
}
