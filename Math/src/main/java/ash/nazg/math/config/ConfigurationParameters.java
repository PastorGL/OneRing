package ash.nazg.math.config;

import ash.nazg.config.tdl.Description;

public final class ConfigurationParameters {
    @Description("Column with a Double to use as series source")
    public static final String DS_CALC_COLUMN = "calc.column";
    @Description("The mathematical function to perform")
    public static final String OP_CALC_FUNCTION = "calc.function";
    @Description("An optional constant value for the selected function")
    public static final String OP_CALC_CONST = "calc.const";

    @Description("Generated column with a result of the mathematical function")
    public static final String GEN_RESULT = "_result";
}
