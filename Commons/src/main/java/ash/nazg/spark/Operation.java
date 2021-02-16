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

import java.util.*;

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
}
