package ash.nazg.config.tdl;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.metadata.DefinitionMeta;
import ash.nazg.storage.metadata.AdapterMeta;

import java.lang.reflect.Constructor;
import java.util.Arrays;

public class AdapterResolver {
    private final String dsName;
    private final AdapterMeta meta;
    private final TaskDefinitionLanguage.Definitions config;

    public AdapterResolver(String dsName, AdapterMeta meta, TaskDefinitionLanguage.Definitions config) {
        this.dsName = dsName;
        this.meta = meta;
        this.config = config;
    }

    public <T> T definition(String key) {
        DefinitionMeta defMeta = meta.settings.get(key);

        if (defMeta == null) {
            throw new InvalidConfigValueException("Invalid property '" + key + "' of adapter '" + meta.name + "'");
        }

        Class<T> clazz;
        try {
            clazz = (Class<T>) Class.forName(defMeta.type);
        } catch (ClassNotFoundException e) {
            throw new InvalidConfigValueException("Cannot resolve class '" + defMeta.type + "' for property '" + key + "' of adapter '" + meta.name + "'");
        }

        String value = config.get(key + "." + dsName);
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
                throw new InvalidConfigValueException("Bad numeric value '" + value + "' for '" + clazz.getSimpleName() + "' property '" + key + "' of adapter '" + meta.name + "'");
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

        throw new InvalidConfigValueException("Improper type '" + clazz.getName() + "' of a property '" + key + "' of adapter '" + meta.name + "'");
    }
}
