/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.columns;

public class AverageFunction extends ColumnsFunction {
    public AverageFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int[] columnsForCalculation, Double shift) {
        super(inputDelimiter, outputDelimiter, outputColumns, columnsForCalculation, shift);
    }

    @Override
    public String[] calcLine(String[] row) {
        String[] out = new String[outputColumns.length];

        for (int i = 0; i < outputColumns.length; i++) {
            if (outputColumns[i] >= 0) {
                out[i] = row[outputColumns[i]];
            } else {
                double result = (_const == null) ? 0.D : _const;
                for (int column : columnsForCalculation) {
                    result += Double.parseDouble(row[column]);
                }
                out[i] = Double.toString(result / columnsForCalculation.length);
            }
        }

        return out;
    }
}
