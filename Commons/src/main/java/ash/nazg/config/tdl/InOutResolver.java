package ash.nazg.config.tdl;

import java.util.Set;
import java.util.stream.Collectors;

public class InOutResolver {
    private final LayerResolver inputLayer;
    private final LayerResolver outputLayer;

    private final Set<String> wildcardOutputs;

    public InOutResolver(TaskDefinitionLanguage.Task taskConfig) {
        inputLayer = new LayerResolver(taskConfig.foreignLayer(Constants.INPUT_LAYER));

        outputLayer = new LayerResolver(taskConfig.foreignLayer(Constants.OUTPUT_LAYER));
        wildcardOutputs = outputLayer.layerConfig.keySet().stream()
                .filter(ds -> ds.endsWith("*"))
                .map(ds -> {
                    String[] splits = ds.split("\\.");

                    return splits[splits.length - 1].substring(0, ds.length() - 1);
                })
                .collect(Collectors.toSet());
    }

    public String inputPathNonLocal(String name) {
        return inputLayer.get(Constants.PATH) + "/" + name;
    }

    public String inputPath(String name) {
        String path = inputLayer.get(Constants.PATH_PREFIX + name);

        if (path == null) {
            return inputLayer.get(Constants.PATH_PREFIX + Constants.DEFAULT_DS) + "/" + name;
        }

        return path;
    }

    public String outputPathNonLocal(String name) {
        return outputLayer.get(Constants.PATH) + "/" + name;
    }

    public String outputPath(String name) {
        String path = outputLayer.get(Constants.PATH_PREFIX + name);

        if (path == null) {
            for (String wild : wildcardOutputs) {
                if (name.startsWith(wild)) {
                    return outputLayer.get(Constants.PATH_PREFIX + wild) + "/" + name;
                }
            }

            return outputLayer.get(Constants.PATH_PREFIX + Constants.DEFAULT_DS) + "/" + name;
        }

        return path;
    }
}
