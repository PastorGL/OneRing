/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.columns;

public class EqualityFunction extends ColumnsMathFunction {
    public EqualityFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int[] columnsForCalculation, Double threshold) {
        super(inputDelimiter, outputDelimiter, outputColumns, columnsForCalculation, threshold);
    }

    @Override
    public String[] calcLine(String[] row) {
        String[] out = new String[outputColumns.length];

        for (int i = 0; i < outputColumns.length; i++) {
            if (outputColumns[i] >= 0) {
                out[i] = row[outputColumns[i]];
            } else {
                String result = "1";

                int value0 = columnsForCalculation[0];
                for (int j = 1; j < columnsForCalculation.length; j++) {
                    int value1 = columnsForCalculation[j];

                    if (_const == null) {
                        if (Double.compare(Double.parseDouble(row[value0]), Double.parseDouble(row[value1])) != 0) {
                            result = "0";
                            break;
                        }
                    } else {
                        if (Math.abs(Double.parseDouble(row[value0]) - Double.parseDouble(row[value1])) >= _const) {
                            result = "0";
                            break;
                        }
                    }

                    value0 = value1;
                }

                out[i] = result;
            }
        }

        return out;
    }
}
