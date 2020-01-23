package ash.nazg.storage;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

public interface InputAdapter extends StorageAdapter {
    void setContext(JavaSparkContext ctx);

    JavaRDDLike load(String path) throws Exception;
}
