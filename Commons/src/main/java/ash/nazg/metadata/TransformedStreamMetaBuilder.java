/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.metadata;

public class TransformedStreamMetaBuilder {
    private final TransformedStreamMeta meta;

    public TransformedStreamMetaBuilder() {
        this.meta = new TransformedStreamMeta();
    }

    public TransformedStreamMetaBuilder genCol(String colName, String colDescr) {
        meta.streams.generated.put(colName, colDescr);

        return this;
    }

    public TransformedStreamMeta build() {
        return meta;
    }
}
