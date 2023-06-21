/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.metadata;

import java.util.HashMap;
import java.util.Map;

public class NamedStreamsMeta extends DataStreamsMeta {
    public final Map<String, DataStreamMeta> streams;

    NamedStreamsMeta() {
        this.streams = new HashMap<>();
    }
}
