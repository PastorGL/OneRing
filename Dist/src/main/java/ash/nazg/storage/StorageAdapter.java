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
import ash.nazg.storage.metadata.AdapterMeta;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class StorageAdapter {
    public final AdapterMeta meta;

    protected JavaSparkContext context;
    protected StreamResolver dsResolver;
    protected LayerResolver distResolver;
    protected String dsName;

    public StorageAdapter() {
        this.meta = meta();
    }

    public void initialize(JavaSparkContext ctx) {
        this.context = ctx;
    }

    protected void configure(String name, TaskDefinitionLanguage.Task taskConfig) throws InvalidConfigValueException {
        this.dsResolver = new StreamResolver(taskConfig.streams);
        this.distResolver = new LayerResolver(taskConfig.foreignLayer(Constants.DIST_LAYER));
        this.dsName = name;

        configure();
    }

    protected abstract AdapterMeta meta();

    abstract protected void configure() throws InvalidConfigValueException;
}
