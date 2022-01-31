/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import scala.Tuple2;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ReachOperationTest {
    @Test
    public void reachTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/test.reach.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> dataset = (JavaRDD<Text>) ret.get("result");

            Map<String, Double> resMap = dataset.mapToPair(t -> {
                String[] s = t.toString().split("\t", 2);

                return new Tuple2<>(s[0], Double.parseDouble(s[1]));
            }).collectAsMap();

            assertEquals(1.0D, resMap.get("gid-all"), 1.E-9D);
            assertEquals(0.1D, resMap.get("gid-onlyone"), 1.E-9D);
            assertEquals(0.6D, resMap.get("gid-some"), 1.E-9D);

        }
    }
}
