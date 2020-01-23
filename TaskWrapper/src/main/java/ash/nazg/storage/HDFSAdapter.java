package ash.nazg.storage;

import java.util.regex.Pattern;

public class HDFSAdapter {
    public static final Pattern PATTERN = Pattern.compile("^hdfs://([^/]+)/(.+)");
}
