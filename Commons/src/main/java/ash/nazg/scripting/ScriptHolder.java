/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.scripting;

import java.util.HashMap;
import java.util.Map;

public class ScriptHolder {
    public final String script;

    public final Map<String, Object> variables = new HashMap<>();
    public final VariablesContext options = new VariablesContext();

    public ScriptHolder(String script, Map variables) {
        this.script = script;
        this.variables.putAll(variables);
    }

    public void setOption(String option, Object value) {
        options.put(option, value);
    }
}
