/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.columns;

public class PowerMeanFunction extends ColumnsFunction {
    public PowerMeanFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int[] columnsForCalculation, double pow) {
        super(inputDelimiter, outputDelimiter, outputColumns, columnsForCalculation, pow);
    }

    @Override
    public String[] calcLine(String[] row) {
        String[] out = new String[outputColumns.length];

        for (int i = 0; i < outputColumns.length; i++) {
            if (outputColumns[i] >= 0) {
                out[i] = row[outputColumns[i]];
            } else {
                double result = 0.D;
                for (int column : columnsForCalculation) {
                    result += Math.pow(Double.parseDouble(row[column]), _const);
                }
                out[i] = Double.toString(Math.pow(result / columnsForCalculation.length, 1.D / _const));
            }
        }

        return out;
    }
}
