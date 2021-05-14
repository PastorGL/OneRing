package ash.nazg.config.tdl;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StreamResolver {
    private final TaskDefinitionLanguage.DataStreams dsConfig;
    private final List<String> variableStreams;
    private final List<String> wildcardStreams;

    public StreamResolver(TaskDefinitionLanguage.DataStreams dsConfig) {
        this.dsConfig = dsConfig;

        this.variableStreams = dsConfig.keySet().stream()
                .filter(ds -> Constants.REP_VAR.matcher(ds).find())
                .collect(Collectors.toList());
        this.wildcardStreams = dsConfig.keySet().stream()
                .filter(ds -> ds.endsWith("*"))
                .map(ds -> ds.substring(0, ds.length() - 1))
                .collect(Collectors.toList());
    }

    public Map<String, Integer> inputColumns(String name) {
        TaskDefinitionLanguage.DataStream ds = dsConfig.getOrDefault(name, null);
        if ((ds == null) || (ds.input == null) || StringUtils.isEmpty(ds.input.columns)) {
            ds = tryDynamicStream(name);
            if ((ds == null) || (ds.input == null) || StringUtils.isEmpty(ds.input.columns)) {
                return null;
            }
        }

        String[] columns = dsConfig.task.arrayValue(ds.input.columns);
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0, j = 1; i < columns.length; i++, j++) {
            String rawName = columns[i];
            if ("_".equals(rawName)) {
                rawName = "_" + j + "_";
            }
            map.put(name + "." + rawName, i);
        }

        return map;
    }

    public String[] rawInputColumns(String name) {
        TaskDefinitionLanguage.DataStream ds = dsConfig.getOrDefault(name, null);
        if ((ds == null) || (ds.input == null) || StringUtils.isEmpty(ds.input.columns)) {
            ds = tryDynamicStream(name);
            if ((ds == null) || (ds.input == null)) {
                return null;
            }
        }

        return dsConfig.task.arrayValue(ds.input.columns);
    }

    public String[] outputColumns(String name) {
        TaskDefinitionLanguage.DataStream ds = dsConfig.getOrDefault(name, null);
        if ((ds == null) || (ds.output == null) || StringUtils.isEmpty(ds.output.columns)) {
            ds = tryDynamicStream(name);
            if ((ds == null) || (ds.output == null)) {
                return null;
            }
        }

        return dsConfig.task.arrayValue(ds.output.columns);
    }

    public char inputDelimiter(String name) {
        TaskDefinitionLanguage.DataStream ds = dsConfig.getOrDefault(name, null);
        if ((ds == null) || (ds.input == null) || StringUtils.isEmpty(ds.input.delimiter)) {
            ds = tryDynamicStream(name);
            if ((ds == null) || (ds.input == null) || StringUtils.isEmpty(ds.input.delimiter)) {
                ds = dsConfig.get(Constants.DEFAULT_DS);
            }
        }

        if (StringUtils.isEmpty(ds.input.delimiter)) {
            return Constants.DEFAULT_DELIMITER;
        }

        return dsConfig.task.value(ds.input.delimiter).charAt(0);
    }

    public char outputDelimiter(String name) {
        TaskDefinitionLanguage.DataStream ds = dsConfig.getOrDefault(name, null);
        if ((ds == null) || (ds.output == null) || StringUtils.isEmpty(ds.output.delimiter)) {
            ds = tryDynamicStream(name);
            if ((ds == null) || (ds.output == null) || StringUtils.isEmpty(ds.output.delimiter)) {
                ds = dsConfig.get(Constants.DEFAULT_DS);
            }
        }

        if (StringUtils.isEmpty(ds.output.delimiter)) {
            return Constants.DEFAULT_DELIMITER;
        }

        return dsConfig.task.value(ds.output.delimiter).charAt(0);
    }

    public int inputParts(String name) {
        TaskDefinitionLanguage.DataStream ds = dsConfig.getOrDefault(name, null);
        if ((ds == null) || (ds.input == null) || StringUtils.isEmpty(ds.input.partCount)) {
            ds = tryDynamicStream(name);
            if ((ds == null) || (ds.input == null) || StringUtils.isEmpty(ds.input.partCount)) {
                return -1;
            }
        }

        return Integer.parseInt(dsConfig.task.value(ds.input.partCount));
    }

    public int outputParts(String name) {
        TaskDefinitionLanguage.DataStream ds = dsConfig.getOrDefault(name, null);
        if ((ds == null) || (ds.output == null) || StringUtils.isEmpty(ds.output.partCount)) {
            ds = tryDynamicStream(name);
            if ((ds == null) || (ds.output == null) || StringUtils.isEmpty(ds.output.partCount)) {
                return -1;
            }
        }

        return Integer.parseInt(dsConfig.task.value(ds.output.partCount));
    }

    private TaskDefinitionLanguage.DataStream tryDynamicStream(String name) {
        for (String varDs : variableStreams) {
            if (name.equals(dsConfig.task.value(varDs))) {
                return dsConfig.get(varDs);
            }
        }
        for (String wildDs : wildcardStreams) {
            if (name.startsWith(wildDs)) {
                return dsConfig.get(wildDs);
            }
        }

        return null;
    }

    public String inputPath(String name) {
        TaskDefinitionLanguage.DataStream ds = dsConfig.getOrDefault(name, null);
        if ((ds == null) || (ds.input == null) || StringUtils.isEmpty(ds.input.path)) {
            ds = tryDynamicStream(name);
            if ((ds == null) || (ds.input == null) || StringUtils.isEmpty(ds.input.path)) {
                ds = dsConfig.get(Constants.DEFAULT_DS);

                return dsConfig.task.value(ds.input.path) + "/" + name + "/*";
            }
        }

        return dsConfig.task.value(ds.input.path);
    }

    public String outputPath(String name) {
        TaskDefinitionLanguage.DataStream ds = dsConfig.getOrDefault(name, null);
        if ((ds == null) || (ds.output == null) || StringUtils.isEmpty(ds.output.path)) {
            ds = tryDynamicStream(name);
            if ((ds == null) || (ds.output == null) || StringUtils.isEmpty(ds.output.path)) {
                ds = dsConfig.get(Constants.DEFAULT_DS);

                return dsConfig.task.value(ds.output.path) + "/" + name;
            }
        }

        return dsConfig.task.value(ds.output.path);
    }
}
