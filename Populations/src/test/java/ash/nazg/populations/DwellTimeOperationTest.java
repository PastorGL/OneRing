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


public class DwellTimeOperationTest {
    @Test
    public void dwellTimeTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/test.dwellTime.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> dataset = (JavaRDD<Text>) ret.get("result");

            Map<String, Double> resMap = dataset.mapToPair(t -> {
                String[] s = t.toString().split("\t", 2);

                return new Tuple2<>(s[0], Double.parseDouble(s[1]));
            }).collectAsMap();


            assertEquals(0.36666666666, resMap.get("cell1"), 1E-06);
            assertEquals(0.3D, resMap.get("cell2"), 1E-06);
            assertEquals(0.75D, resMap.get("cell3"), 1E-06);
        }
    }
}
