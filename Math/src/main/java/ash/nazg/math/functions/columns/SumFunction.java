package ash.nazg.math.functions.columns;

public class SumFunction extends ColumnsMathFunction {
    public SumFunction(char inputDelimiter, char outputDelimiter, int[] outputColumns, int[] columnsForCalculation, Double _const) {
        super(inputDelimiter, outputDelimiter, outputColumns, columnsForCalculation, _const);
    }

    @Override
    public String[] calcLine(String[] row) {
        String[] out = new String[outputColumns.length];

        for (int i = 0; i < outputColumns.length; i++) {
            if (outputColumns[i] > 0) {
                out[i] = row[outputColumns[i]];
            } else {
                double result = (_const != null) ? _const : 0.D;
                for (int column : columnsForCalculation) {
                    result += new Double(row[column]);
                }
                out[i] = Double.toString(result);
            }
        }

        return out;
    }
}
