/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.WrapperConfig;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class WrapperBase {
    public static final String APP_NAME = "One Ring";

    protected JavaSparkContext context;
    protected WrapperConfig wrapperConfig;

    public WrapperBase(JavaSparkContext context, WrapperConfig wrapperConfig) {
        this.context = context;
        this.wrapperConfig = wrapperConfig;
    }
}