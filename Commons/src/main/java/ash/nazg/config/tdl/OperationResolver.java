package ash.nazg.config.tdl;

import ash.nazg.config.InvalidConfigValueException;

import java.lang.reflect.Constructor;
import java.util.Arrays;

public class OperationResolver {
    private final TaskDescriptionLanguage.Operation opDesc;
    private final TaskDefinitionLanguage.Operation opConfig;

    public OperationResolver(TaskDescriptionLanguage.Operation opDesc, TaskDefinitionLanguage.Operation opConfig) {
        this.opDesc = opDesc;
        this.opConfig = opConfig;
    }

    public <T> T definition(String key) {
        TaskDescriptionLanguage.DefBase db = opDesc.definitions.get(key);
        if (db == null) {
            for (String def : opDesc.definitions.keySet()) {
                if (key.startsWith(def)) {
                    db = opDesc.definitions.get(def);
                    break;
                }
            }
        }
        if (db == null) {
            throw new InvalidConfigValueException("Invalid property '" + key + "' of operation '" + opConfig.name + "'");
        }

        Class<T> clazz = db.clazz;

        String value = opConfig.definitions.get(key);
        if (value == null || value.isEmpty()) {
            value = db.defaults;
        }

        value = opConfig.task.value(value);
        if (value == null) {
            return null;
        }

        if (Number.class.isAssignableFrom(clazz)) {
            try {
                Constructor c = clazz.getConstructor(String.class);
                return (T) c.newInstance(value);
            } catch (Exception e) {
                throw new InvalidConfigValueException("Bad numeric value '" + value + "' for '" + clazz.getSimpleName() + "' property '" + key + "' of operation '" + opConfig.name + "'");
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

        throw new InvalidConfigValueException("Improper type '" + clazz.getName() + "' of a property '" + key + "' of operation '" + opConfig.name + "'");
    }

    public String[] positionalInputs() {
        return opConfig.task.arrayValue(opConfig.inputs.positionalNames);
    }

    public String positionalInput(int index) {
        return positionalInputs()[index];
    }

    public String[] positionalOutputs() {
        return opConfig.task.arrayValue(opConfig.outputs.positionalNames);
    }

    public String positionalOutput(int index) {
        return positionalOutputs()[index];
    }

    public String namedInput(String name) {
        return opConfig.task.value(opConfig.inputs.named.get(name));
    }

    public String namedOutput(String name) {
        return opConfig.task.value(opConfig.outputs.named.get(name));
    }
}
