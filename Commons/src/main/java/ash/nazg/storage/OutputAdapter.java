package ash.nazg.storage;

import org.apache.spark.api.java.JavaRDDLike;

public interface OutputAdapter extends StorageAdapter {
    void save(String path, JavaRDDLike rdd);
}
