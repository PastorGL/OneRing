package ash.nazg.spark;

import ash.nazg.config.tdl.TaskDescriptionLanguage;
import org.apache.spark.api.java.JavaRDDLike;

import java.io.Serializable;
import java.util.Map;

public interface OpInfo extends Serializable {
    String verb();

    TaskDescriptionLanguage.Operation description();

    Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) throws Exception;
}
