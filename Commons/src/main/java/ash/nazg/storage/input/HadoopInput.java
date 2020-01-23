package ash.nazg.storage.input;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.storage.HadoopAdapter;
import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.InputAdapter;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

@SuppressWarnings("unused")
public class HadoopInput extends HadoopAdapter implements InputAdapter {
    private int partCount;

    private JavaSparkContext ctx;

    @Override
    public void setProperties(String name, WrapperConfig wrapperConfig) throws InvalidConfigValueException {
        partCount = wrapperConfig.inputParts(name);
    }

    @Override
    public void setContext(JavaSparkContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public JavaRDDLike load(String path) {
        return ctx.textFile(path, Math.max(partCount, 1));
    }
}
