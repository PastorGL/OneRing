/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import org.apache.spark.api.java.JavaRDDLike;

public interface OutputAdapter extends StorageAdapter {
    void save(String path, JavaRDDLike rdd);
}
