/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.output;

import ash.nazg.config.DataStreamsConfig;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.storage.HadoopAdapter;
import ash.nazg.config.WrapperConfig;
import ash.nazg.storage.OutputAdapter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("unused")
public class HadoopOutput extends HadoopAdapter implements OutputAdapter {
    private char delimiter;

    public void setProperties(String outputName, WrapperConfig wrapperConfig) throws InvalidConfigValueException {
        DataStreamsConfig adapterConfig = new DataStreamsConfig(wrapperConfig.getProperties(), null, null, Collections.singleton(outputName), Collections.singleton(outputName), null);

        delimiter = adapterConfig.outputDelimiter(outputName);
    }

    @Override
    public void save(String path, JavaRDDLike rdd) {
        if (rdd instanceof JavaPairRDD) {
            final String _delimiter = "" + delimiter;
            ((JavaPairRDD<Object, Object>) rdd).mapPartitions(it -> {
                List<Text> ret = new ArrayList<>();

                while (it.hasNext()) {
                    Tuple2 v = it.next();

                    ret.add(new Text(v._1 + _delimiter + v._2));
                }

                return ret.iterator();
            }).saveAsTextFile(path);
        }

        if (rdd instanceof JavaRDD) {
            rdd.saveAsTextFile(path);
        }
    }
}
