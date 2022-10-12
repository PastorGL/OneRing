/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.data;

import ash.nazg.metadata.TransformMeta;

public interface Transform {
    TransformMeta meta();

    StreamConverter converter();
}
