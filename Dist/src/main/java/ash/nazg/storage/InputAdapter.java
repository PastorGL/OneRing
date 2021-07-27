/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.AdapterResolver;
import ash.nazg.config.tdl.Constants;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;

public abstract class InputAdapter extends StorageAdapter {
    protected AdapterResolver inputResolver;

    public abstract JavaRDD<Text> load(String path) throws Exception;

    public void configure(String name, TaskDefinitionLanguage.Task taskConfig) throws InvalidConfigValueException {
        inputResolver = new AdapterResolver(name, meta, taskConfig.foreignLayer(Constants.INPUT_LAYER));

        super.configure(name, taskConfig);
    }
}
