package ash.nazg.config.tdl;

public class LayerResolver {
    private final TaskDefinitionLanguage.Definitions layerConfig;

    public LayerResolver(TaskDefinitionLanguage.Definitions layerConfig) {
        this.layerConfig = layerConfig;
    }

    public String get(String key) {
        String value = layerConfig.get(key);
        if (value == null || value.isEmpty()) {
            return null;
        }

        return layerConfig.task.value(value);
    }

    public String get(String key, String defaults) {
        String value = layerConfig.get(key);
        if (value == null || value.isEmpty()) {
            value = defaults;
        }

        return layerConfig.task.value(value);
    }

    public String[] getArray(String key) {
        String value = layerConfig.get(key);
        if (value == null || value.isEmpty()) {
            return null;
        }

        return layerConfig.task.arrayValue(value);
    }
}
