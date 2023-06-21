/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.metadata;

import ash.nazg.metadata.DataStreamMeta;
import ash.nazg.metadata.DataStreamsMeta;
import ash.nazg.metadata.Origin;

public class TransformedStreamMeta extends DataStreamsMeta {
    public final DataStreamMeta streams;

    TransformedStreamMeta() {
        this.streams = new DataStreamMeta(null, null, false, Origin.GENERATED, null);
    }
}
