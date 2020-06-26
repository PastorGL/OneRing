/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import java.util.*;

public class DataStreamsConfig extends PropertiesConfig {
    public static final String DS_INPUT_COLUMNS_PREFIX = "ds.input.columns.";
    public static final String DS_INPUT_DELIMITER_PREFIX = "ds.input.delimiter.";
    public static final String DS_OUTPUT_COLUMNS_PREFIX = "ds.output.columns.";
    public static final String DS_OUTPUT_DELIMITER_PREFIX = "ds.output.delimiter.";

    public final Map<String, Map<String, Integer>> inputColumns = new HashMap<>();
    public final Map<String, String[]> inputColumnsRaw = new HashMap<>();
    public final Map<String, String[]> outputColumns = new HashMap<>();
    private final Map<String, Character> inputDelimiters = new HashMap<>();
    private final Map<String, Character> outputDelimiters = new HashMap<>();

    public DataStreamsConfig(Properties sourceConfig,
                             Collection<String> allInputs, Collection<String> columnBasedInputs,
                             Collection<String> allOutputs, Collection<String> columnBasedOutputs,
                             Map<String, String[]> generatedColumns) throws InvalidConfigValueException {
        setProperties(sourceConfig);

        if (allInputs != null) {
            for (String allInput : allInputs) {
                if (!inputDelimiters.containsKey(allInput)) {
                    String delimiter = getProperty(DS_INPUT_DELIMITER_PREFIX + allInput);

                    char del = ((delimiter == null) || delimiter.isEmpty())
                            ? getDsInputDelimiter()
                            : delimiter.charAt(0);

                    inputDelimiters.put(allInput, del);
                }
            }
        }

        if (columnBasedInputs != null) {
            for (String input : columnBasedInputs) {
                if (!inputColumnsRaw.containsKey(input)) {
                    String[] cols = getArray(DS_INPUT_COLUMNS_PREFIX + input);

                    String[] _cols_;
                    if (cols == null) {
                        _cols_ = new String[0];
                    } else {
                        int columnNumber = 1;
                        _cols_ = new String[cols.length];
                        for (int i = 0; i < cols.length; i++) {
                            _cols_[i] = cols[i].equals("_")
                                    ? "_" + columnNumber + "_"
                                    : cols[i]
                            ;
                            columnNumber++;
                        }
                        inputColumnsRaw.put(input, _cols_);
                    }

                    Map<String, Integer> inputMap = new LinkedHashMap<>();

                    for (int i = 0; i < _cols_.length; i++) {
                        String columnName = _cols_[i];

                        if (inputMap.containsKey(columnName)) {
                            throw new InvalidConfigValueException("Duplicate column reference '" + columnName + "' in input definition '" + input + "'");
                        }

                        inputMap.put(input + "." + columnName, i);
                    }

                    inputColumns.put(input, inputMap);
                }
            }
        }

        if (allOutputs != null) {
            for (String output : allOutputs) {
                String delimiter = getProperty(DS_OUTPUT_DELIMITER_PREFIX + output);

                char del = ((delimiter == null) || delimiter.isEmpty())
                        ? getDsOutputDelimiter()
                        : delimiter.charAt(0);

                outputDelimiters.put(output, del);
            }
        }

        if (columnBasedOutputs != null) {
            for (String o : columnBasedOutputs) {
                String[] columns = getArray(DS_OUTPUT_COLUMNS_PREFIX + o);
                columns = (columns == null) ? new String[0] : columns;

                String[] outputGeneratedColumns = null;
                if (generatedColumns != null) {
                    outputGeneratedColumns = generatedColumns.get(o);
                }

                checkOutputColumns:
                for (String outputColumn : columns) {
                    if ((outputGeneratedColumns != null) && (outputColumn.startsWith("_"))) {
                        for (String g : outputGeneratedColumns) {
                            if (g.equals(outputColumn)) {
                                continue checkOutputColumns;
                            }
                            if (g.endsWith("*") && (outputColumn.startsWith(g.substring(0, g.length() - 2)))) {
                                continue checkOutputColumns;
                            }
                        }

                        throw new InvalidConfigValueException("Output '" + o + "' refers to an unknown generated column '" + outputColumn + "'");
                    }

                    if ((columnBasedInputs != null) && (columnBasedInputs.size() > 0)) {
                        String[] column = outputColumn.split("\\.", 2);

                        String[] rawColumns = inputColumnsRaw.get(column[0]);
                        if (rawColumns == null) {
                            throw new InvalidConfigValueException("Input '" + column[0] + "' required for the output '" + o + "' wasn't defined or isn't column-based");
                        }

                        if (!Arrays.asList(rawColumns).contains(column[1])) {
                            throw new InvalidConfigValueException("Column '" + column[1] + "' of output '" + o + "' doesn't exist in the input '" + column[0] + "'");
                        }
                    }
                }

                outputColumns.put(o, columns);
            }
        }
    }

    public final char defaultInputDelimiter() {
        return getDsInputDelimiter();
    }

    public final char defaultOutputDelimiter() {
        return getDsOutputDelimiter();
    }

    public char inputDelimiter(String input) {
        return inputDelimiters.get(input);
    }

    public char outputDelimiter(String output) {
        return outputDelimiters.get(output);
    }
}
