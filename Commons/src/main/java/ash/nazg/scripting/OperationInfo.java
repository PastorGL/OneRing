/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.scripting;

import ash.nazg.metadata.OperationMeta;

public class OperationInfo {
    public final Class<? extends Operation> opClass;
    public final OperationMeta meta;

    public OperationInfo(Class<? extends Operation> opClass, OperationMeta meta) {
        this.opClass = opClass;
        this.meta = meta;
    }
}
