/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.math.config.CalcFunction;
import ash.nazg.math.functions.columns.*;
import ash.nazg.spark.Operation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.sparkproject.guava.primitives.Ints;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static ash.nazg.math.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class ColumnsMathOperation extends Operation {
    public static final String OP_CALC_COLUMNS = "calc.columns";

    private String inputName;

    private String outputName;

    private ColumnsMathFunction mathFunc;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("columnsMath", "This operation performs one of the predefined mathematical operations on selected set of columns" +
                " inside each input row, generating a column with a result. Data type is implied Double",

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD with a set of columns of type Double",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_CALC_COLUMNS, "Columns with source values", String[].class)
                        .def(OP_CALC_FUNCTION, "The mathematical function to perform", CalcFunction.class)
                        .def(OP_CALC_CONST, "An optional constant value for the selected function", Double.class,
                                null, "By default the constant isn't set")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD with calculation result",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .genCol(GEN_RESULT, "Generated column with a result of the mathematical function")
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        char inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputName = opResolver.positionalOutput(0);
        char outputDelimiter = dsResolver.outputDelimiter(outputName);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);

        String[] calcColumns = opResolver.definition(OP_CALC_COLUMNS);
        Set<Integer> separatedProp = new HashSet<>();
        if (calcColumns.length == 1) {
            final String colTemplate = calcColumns[0].endsWith("*") ? calcColumns[0].substring(0, calcColumns[0].length() - 1) : calcColumns[0];
            inputColumns.forEach((key, value) -> {
                if (key.startsWith(colTemplate)) {
                    separatedProp.add(value);
                }
            });
        } else {
            for (String column : calcColumns) {
                separatedProp.add(inputColumns.get(column));
            }
        }
        int[] columnsForCalculation = Ints.toArray(separatedProp);
        if (columnsForCalculation.length == 0) {
            throw new InvalidConfigValueException("Operation '" + name + "' requires at least one column in " + OP_CALC_COLUMNS + " set either explicitly of as a wildcard");
        }

        String[] outputColumns = dsResolver.outputColumns(outputName);
        int[] outputCols = new int[outputColumns.length];
        int i = 0;
        for (String outputColumn : outputColumns) {
            if (!outputColumn.equals(GEN_RESULT)) {
                outputCols[i++] = inputColumns.get(outputColumn);
            } else {
                outputCols[i++] = -1;
            }
        }

        CalcFunction cf = opResolver.definition(OP_CALC_FUNCTION);
        switch (cf) {
            case SUM: {
                Double _const = opResolver.definition(OP_CALC_CONST);
                mathFunc = new SumFunction(inputDelimiter, outputDelimiter, outputCols, columnsForCalculation, _const);
                break;
            }
            case POWERMEAN: {
                Double pow = opResolver.definition(OP_CALC_CONST);
                if (pow == null) {
                    throw new InvalidConfigValueException("POWERMEAN function of the operation '" + name + "' requires " + OP_CALC_CONST + " set");
                }
                mathFunc = new PowerMeanFunction(inputDelimiter, outputDelimiter, outputCols, columnsForCalculation, pow);
                break;
            }
            case AVERAGE: {
                Double shift = opResolver.definition(OP_CALC_CONST);
                mathFunc = new AverageFunction(inputDelimiter, outputDelimiter, outputCols, columnsForCalculation, shift);
                break;
            }
            case RMS: {
                mathFunc = new PowerMeanFunction(inputDelimiter, outputDelimiter, outputCols, columnsForCalculation, 2.D);
                break;
            }
            case MIN: {
                Double floor = opResolver.definition(OP_CALC_CONST);
                mathFunc = new MinFunction(inputDelimiter, outputDelimiter, outputCols, columnsForCalculation, floor);
                break;
            }
            case MAX: {
                Double ceil = opResolver.definition(OP_CALC_CONST);
                mathFunc = new MaxFunction(inputDelimiter, outputDelimiter, outputCols, columnsForCalculation, ceil);
                break;
            }
            case MUL: {
                Double _const = opResolver.definition(OP_CALC_CONST);
                mathFunc = new MulFunction(inputDelimiter, outputDelimiter, outputCols, columnsForCalculation, _const);
                break;
            }
            case DIV: {
                Double _const = opResolver.definition(OP_CALC_CONST);
                mathFunc = new DivFunction(inputDelimiter, outputDelimiter, outputCols, columnsForCalculation, _const);
                break;
            }
            case EQUALITY: {
                Double _const = opResolver.definition(OP_CALC_CONST);
                mathFunc = new EqualityFunction(inputDelimiter, outputDelimiter, outputCols, columnsForCalculation, _const);
                break;
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD output = input.get(inputName).mapPartitions(mathFunc);

        return Collections.singletonMap(outputName, output);
    }
}
