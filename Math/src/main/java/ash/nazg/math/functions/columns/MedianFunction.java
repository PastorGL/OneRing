package ash.nazg.math.functions.columns;

import java.util.Arrays;

public class MedianFunction extends ColumnsMathFunction {
    public MedianFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int[] columnsForCalculation) {
        super(inputDelimiter, outputDelimiter, outputColumns, columnsForCalculation, null);
    }

    @Override
    public String[] calcLine(String[] row) {
        String[] out = new String[outputColumns.length];

        for (int i = 0; i < outputColumns.length; i++) {
            if (outputColumns[i] >= 0) {
                out[i] = row[outputColumns[i]];
            } else {
                int size = columnsForCalculation.length;
                double[] result = new double[size];

                for (int j = 0; j < size; j++) {
                    int column = columnsForCalculation[j];
                    result[j] = Double.parseDouble(row[column]);
                }
                Arrays.sort(result);

                int m = size >> 1;
                out[i] = String.valueOf(
                        (size % 2 == 0)
                                ? (result[m] + result[m - 1]) / 2.D
                                : result[m]
                );
            }
        }

        return out;
    }
}
