/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.data;

import ash.nazg.metadata.TransformMeta;

public class TransformInfo {
    public final Class<? extends Transform> transformClass;
    public final TransformMeta meta;

    public TransformInfo(Class<? extends Transform> transformClass, TransformMeta meta) {
        this.transformClass = transformClass;
        this.meta = meta;
    }
}
