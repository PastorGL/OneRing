/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime.config;

import ash.nazg.config.tdl.Description;

public final class ConfigurationParameters {
    @Description("Source column with a timestamp")
    public static final String DS_SRC_TIMESTAMP_COLUMN = "source.timestamp.column";

    @Description("If set, use this format to parse source timestamp")
    public static final String OP_SRC_TIMESTAMP_FORMAT = "source.timestamp.format";
    @Description("Source timezone default")
    public static final String OP_SRC_TIMEZONE_DEFAULT = "source.timezone.default";
    @Description("If set, use source timezone from this column instead of the default")
    public static final String OP_SRC_TIMEZONE_COL = "source.timezone.col";

    @Description("If set, use this format to output full date")
    public static final String OP_DST_TIMESTAMP_FORMAT = "destination.timestamp.format";
    @Description("Destination timezone default")
    public static final String OP_DST_TIMEZONE_DEFAULT = "destination.timezone.default";
    @Description("If set, use destination timezone from this column instead of the default")
    public static final String OP_DST_TIMEZONE_COL = "destination.timezone.col";
}
