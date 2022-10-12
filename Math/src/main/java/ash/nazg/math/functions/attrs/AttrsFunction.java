/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math.functions.attrs;

import ash.nazg.data.Record;

import java.io.Serializable;

public abstract class AttrsFunction implements Serializable {
    protected final String[] columnsForCalculation;

    public AttrsFunction(String[] columnsForCalculation) {
        this.columnsForCalculation = columnsForCalculation;
    }

    public abstract double calcValue(Record row);
}
