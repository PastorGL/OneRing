/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Constants;
import ash.nazg.config.tdl.LayerResolver;
import ash.nazg.config.tdl.StreamResolver;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.regex.Pattern;

public abstract class StorageAdapter {
    public static final Pattern PATH_PATTERN = Pattern.compile("^([^:]+:/*[^/]+)/(.+)");

    protected JavaSparkContext context;
    protected StreamResolver dsResolver;
    protected LayerResolver distResolver;
    protected String name;

    public void initialize(JavaSparkContext ctx) {
        this.context = ctx;
    }

    protected void configure(String name, TaskDefinitionLanguage.Task taskConfig) throws InvalidConfigValueException {
        this.dsResolver = new StreamResolver(taskConfig.dataStreams);
        this.distResolver = new LayerResolver(taskConfig.foreignLayer(Constants.DIST_LAYER));
        this.name = name;

        configure();
    }

    public abstract Pattern proto();

    abstract protected void configure() throws InvalidConfigValueException;
}
