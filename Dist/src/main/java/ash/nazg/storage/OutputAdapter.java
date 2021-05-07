/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Constants;
import ash.nazg.config.tdl.LayerResolver;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;

public abstract class OutputAdapter extends StorageAdapter {
    protected LayerResolver outputResolver;

    public void configure(String name, TaskDefinitionLanguage.Task taskConfig) throws InvalidConfigValueException {
        outputResolver = new LayerResolver(taskConfig.foreignLayer(Constants.OUTPUT_LAYER));

        super.configure(name, taskConfig);
    }

    public abstract void save(String path, JavaRDD<Text> rdd);
}
