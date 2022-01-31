/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.columns;

public class DivFunction extends ColumnsMathFunction {
    public DivFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int[] columnsForCalculation, Double _const) {
        super(inputDelimiter, outputDelimiter, outputColumns, columnsForCalculation, _const);
    }

    @Override
    public String[] calcLine(String[] row) {
        String[] out = new String[outputColumns.length];

        for (int i = 0; i < outputColumns.length; i++) {
            if (outputColumns[i] >= 0) {
                out[i] = row[outputColumns[i]];
            } else {
                double result = Double.parseDouble(row[columnsForCalculation[0]]);
                for (int j = 1; j < columnsForCalculation.length; j++) {
                    int column = columnsForCalculation[j];
                    result /= Double.parseDouble(row[column]);
                }
                if ((_const != null) && (_const != 0.D)) {
                    result /= _const;
                }
                out[i] = Double.toString(result);
            }
        }

        return out;
    }
}
