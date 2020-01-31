/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

public interface InputAdapter extends StorageAdapter {
    void setContext(JavaSparkContext ctx);

    JavaRDDLike load(String path) throws Exception;
}
