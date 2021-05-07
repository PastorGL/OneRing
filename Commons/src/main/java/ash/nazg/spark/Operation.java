/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.OperationResolver;
import ash.nazg.config.tdl.StreamResolver;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.*;

public abstract class Operation implements Serializable {
    public final TaskDescriptionLanguage.Operation description;

    protected JavaSparkContext ctx;
    protected OperationResolver opResolver;
    protected StreamResolver dsResolver;
    protected String name;

    public Operation() {
        this.description = description();
    }

    public void initialize(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    public void configure(TaskDefinitionLanguage.Operation opConfig, TaskDefinitionLanguage.DataStreams dsConfig) throws InvalidConfigValueException {
        this.opResolver = new OperationResolver(description, opConfig);
        this.dsResolver = new StreamResolver(dsConfig);
        this.name = opConfig.name;

        configure();
    }

    abstract public String verb();

    abstract protected TaskDescriptionLanguage.Operation description();

    abstract protected void configure() throws InvalidConfigValueException;

    protected static List<String> getMatchingInputs(Collection<String> inputNames, String keys) {
        String[] templates = keys.split(",");

        List<String> ds = new ArrayList<>();

        for (String template : templates) {
            if (template.endsWith("*")) {
                template = template.substring(0, template.length() - 2);

                for (String key : inputNames) {
                    if (key.startsWith(template)) {
                        ds.add(key);
                    }
                }
            } else if (inputNames.contains(template)) {
                ds.add(template);
            }
        }

        return ds;
    }

    @SuppressWarnings("rawtypes")
    abstract public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) throws Exception;
}
