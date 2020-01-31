/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.columns;

public class MinFunction extends ColumnsMathFunction {
    public MinFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int[] columnsForCalculation, Double floor) {
        super(inputDelimiter, outputDelimiter, outputColumns, columnsForCalculation, floor);
    }

    @Override
    public String[] calcLine(String[] row) {
        String[] out = new String[outputColumns.length];

        for (int i = 0; i < outputColumns.length; i++) {
            if (outputColumns[i] > 0) {
                out[i] = row[outputColumns[i]];
            } else {
                double result = Double.POSITIVE_INFINITY;
                for (int value : columnsForCalculation) {
                    result = Math.min(result, new Double(row[value]));
                }
                if ((_const != null) && (_const > result)) {
                    result = _const;
                }
                out[i] = Double.toString(result);
            }
        }

        return out;
    }
}
