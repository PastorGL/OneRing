package ash.nazg.config.tdl;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.metadata.DefinitionMeta;
import ash.nazg.config.tdl.metadata.OperationMeta;

import java.lang.reflect.Constructor;
import java.util.Arrays;

public class OperationResolver {
    private final OperationMeta meta;
    private final TaskDefinitionLanguage.Operation config;

    public OperationResolver(OperationMeta meta, TaskDefinitionLanguage.Operation config) {
        this.meta = meta;
        this.config = config;
    }

    public <T> T definition(String key) {
        DefinitionMeta defMeta = meta.definitions.get(key);
        if (defMeta == null) {
            for (String def : meta.definitions.keySet()) {
                if (key.startsWith(def)) {
                    defMeta = meta.definitions.get(def);
                    break;
                }
            }
        }
        if (defMeta == null) {
            throw new InvalidConfigValueException("Invalid property '" + key + "' of operation '" + config.name + "'");
        }

        Class<T> clazz;
        try {
            clazz = (Class<T>) Class.forName(defMeta.type);
        } catch (ClassNotFoundException e) {
            throw new InvalidConfigValueException("Cannot resolve class '" + defMeta.type + "' for property '" + key + "' of operation '" + config.name + "'");
        }

        String value = config.definitions.get(key);
        if (value == null || value.isEmpty()) {
            value = defMeta.defaults;
        }

        value = config.task.value(value);
        if (value == null) {
            return null;
        }

        if (Number.class.isAssignableFrom(clazz)) {
            try {
                Constructor c = clazz.getConstructor(String.class);
                return (T) c.newInstance(value);
            } catch (Exception e) {
                throw new InvalidConfigValueException("Bad numeric value '" + value + "' for '" + clazz.getSimpleName() + "' property '" + key + "' of operation '" + config.name + "'");
            }
        } else if (String.class == clazz) {
            return (T) value;
        } else if (Boolean.class == clazz) {
            return (T) Boolean.valueOf(value);
        } else if (clazz.isEnum()) {
            return (T) Enum.valueOf((Class) clazz, value);
        } else if (String[].class == clazz) {
            return (T) Arrays.stream(value.split(Constants.COMMA)).map(String::trim).toArray(String[]::new);
        }

        throw new InvalidConfigValueException("Improper type '" + clazz.getName() + "' of a property '" + key + "' of operation '" + config.name + "'");
    }

    public String[] positionalInputs() {
        return config.task.arrayValue(config.input.positional);
    }

    public String positionalInput(int index) {
        return positionalInputs()[index];
    }

    public String[] positionalOutputs() {
        return config.task.arrayValue(config.output.positional);
    }

    public String positionalOutput(int index) {
        return positionalOutputs()[index];
    }

    public String namedInput(String name) {
        return config.task.value(config.input.named.get(name));
    }

    public String namedOutput(String name) {
        return config.task.value(config.output.named.get(name));
    }
}
