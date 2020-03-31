/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import ash.nazg.config.DataStreamsConfig;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class Operation implements OpInfo {
    protected JavaSparkContext ctx;
    protected String name;

    protected OperationConfig describedProps;
    protected DataStreamsConfig dataStreamsProps;

    public Operation() {
    }

    public void initialize(String name, JavaSparkContext ctx) {
        this.name = name;
        this.ctx = ctx;
    }

    public void configure(Properties config, Properties variables) throws InvalidConfigValueException {
        describedProps = new OperationConfig(description(), name);
        dataStreamsProps = describedProps.configure(config, variables);
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
