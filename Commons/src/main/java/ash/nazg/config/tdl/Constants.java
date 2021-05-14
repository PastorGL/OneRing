/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import java.util.regex.Pattern;

public class Constants {
    /**
     * Separator for value lists.
     */
    public static final String COMMA = ",";
    /**
     * By default, CSV RDD values are delimited by a TAB
     */
    public static final char DEFAULT_DELIMITER = '\t';
    public static final String DS_INPUT_DELIMITER = "ds.input.delimiter";
    public static final String DS_OUTPUT_DELIMITER = "ds.output.delimiter";
    public static final String DS_INPUT_PATH = "ds.input.path";
    public static final String DS_OUTPUT_PATH = "ds.output.path";
    public static final String REP_SEP = ":";
    public static final Pattern REP_VAR = Pattern.compile("\\{([^:{}]+:?|[^:{}]*?:[^{}]+?)}");
    public static final String DS_INPUT_PREFIX = "ds.input.";
    public static final String DS_OUTPUT_PREFIX = "ds.output.";
    public static final String DS_INPUT_COLUMNS_PREFIX = "ds.input.columns.";
    public static final String DS_INPUT_DELIMITER_PREFIX = "ds.input.delimiter.";
    public static final String DS_OUTPUT_COLUMNS_PREFIX = "ds.output.columns.";
    public static final String DS_OUTPUT_DELIMITER_PREFIX = "ds.output.delimiter.";
    public static final String OP_OPERATION_PREFIX = "op.operation.";
    public static final String OP_INPUTS_PREFIX = "op.inputs.";
    public static final String OP_INPUT_PREFIX = "op.input.";
    public static final String OP_OUTPUTS_PREFIX = "op.outputs.";
    public static final String OP_OUTPUT_PREFIX = "op.output.";
    public static final String OP_DEFINITION_PREFIX = "op.definition.";
    public static final String COLUMN_SUFFIX = ".column";
    public static final String COLUMNS_SUFFIX = ".columns";
    public static final String DS_INPUT_PATH_PREFIX = "ds.input.path.";
    public static final String DS_OUTPUT_PATH_PREFIX = "ds.output.path.";
    public static final String DS_INPUT_PART_COUNT_PREFIX = "ds.input.part_count.";
    public static final String DS_OUTPUT_PART_COUNT_PREFIX = "ds.output.part_count.";
    public static final String DIST_LAYER = "dist";
    public static final String INPUT_LAYER = "input";
    public static final String OUTPUT_LAYER = "output";
    public static final String METRICS_LAYER = "metrics";
    public static final String TASK_INPUT = "task.input";
    public static final String TASK_OUTPUT = "task.output";
    public static final String TASK_OPERATIONS = "task.operations";
    public static final String DIRECTIVE_SIGIL = "$";
    public static final Pattern REP_OPERATIONS = Pattern.compile("(\\$[^{,]+\\{[^}]+?}|\\$[^,]+|[^,]+)");
    public static final String DEFAULT_DS = "_default";
    public static final String METRICS_DS = "_metrics";
    public static final String OUTPUTS_DS = "_outputs";
    public static final String VAR_ITER = "ITER";
}
