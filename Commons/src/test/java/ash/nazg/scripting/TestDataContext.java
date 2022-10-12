package ash.nazg.scripting;

import ash.nazg.data.DataContext;
import org.apache.spark.api.java.JavaSparkContext;

public class TestDataContext extends DataContext {
    public TestDataContext(JavaSparkContext context) {
        super(context);
    }

    @Override
    public String inputPathLocal(String name, String path) {
        return getClass().getResource("/").getPath() + path;
    }
}
