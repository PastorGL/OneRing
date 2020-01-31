/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RangeFilterOperationTest {

    @Test
    public void rangeFilterTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.rangeFilter.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> signalsRDD = (JavaRDD<Text>) ret.get("signals");
            assertEquals(
                    28,
                    signalsRDD.count()
            );

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("filtered1");

            assertEquals(
                    3,
                    resultRDD.count()
            );

            resultRDD = (JavaRDD<Text>) ret.get("filtered2");

            assertEquals(
                    26,
                    resultRDD.count()
            );

            resultRDD = (JavaRDD<Text>) ret.get("filtered3");

            assertEquals(
                    4,
                    resultRDD.count()
            );

            resultRDD = (JavaRDD<Text>) ret.get("filtered4");

            assertEquals(
                    14,
                    resultRDD.count()
            );
        }
    }
}
