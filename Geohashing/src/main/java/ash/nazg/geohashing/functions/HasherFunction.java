/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.geohashing.functions;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Iterator;

public abstract class HasherFunction implements FlatMapFunction<Iterator<Tuple3<Double, Double, Text>>, Tuple2<Text, Text>> {
    protected int level;

    protected HasherFunction(int level) {
        this.level = level;
    }

    abstract public Iterator<Tuple2<Text, Text>> call(Iterator<Tuple3<Double, Double, Text>> signals) throws Exception;
}
