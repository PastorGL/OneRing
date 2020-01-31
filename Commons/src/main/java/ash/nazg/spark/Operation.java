/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.DataStreamsConfig;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Operation implements Serializable {
    protected JavaSparkContext ctx;
    protected String name;

    protected OperationConfig describedProps;
    protected DataStreamsConfig dataStreamsProps;

    public Operation() {
    }

    public Class<? extends OperationConfig> configClass() {
        return OperationConfig.class;
    }

    abstract public String verb();

    abstract public TaskDescriptionLanguage.Operation description();

    public Map.Entry<String, Info> info() {
        String verb = verb();
        Class<? extends Operation> opClass = getClass();
        return new AbstractMap.SimpleImmutableEntry<>(
                verb,
                new Info(verb,
                        opClass,
                        configClass(),
                        description()
                )
        );
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setContext(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    public void setConfig(OperationConfig config) throws InvalidConfigValueException {
        describedProps = config;
        dataStreamsProps = describedProps.dsc;
    }

    public abstract Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) throws Exception;

    public String getName() {
        return name;
    }

    public static class Info {
        public final String verb;
        public final Class<? extends Operation> operationClass;
        public final Class<? extends OperationConfig> configClass;
        public final TaskDescriptionLanguage.Operation description;

        private Info(String verb, Class<? extends Operation> operationClass, Class<? extends OperationConfig> configClass, TaskDescriptionLanguage.Operation description) {
            this.verb = verb;
            this.operationClass = operationClass;
            this.configClass = configClass;
            this.description = description;
        }
    }

    protected static List<JavaRDDLike> getMatchingInputs(Map<String, JavaRDDLike> map, String keys) {
        String[] templates = keys.split(",");

        List<JavaRDDLike> ds = new ArrayList<>();

        for (String template : templates) {
            if (template.endsWith("*")) {
                template = template.substring(0, template.length() - 2);

                for (String key : map.keySet()) {
                    if (key.startsWith(template)) {
                        ds.add(map.get(key));
                    }
                }
            } else if (map.containsKey(template)) {
                ds.add(map.get(template));
            }
        }

        return ds;
    }
}
