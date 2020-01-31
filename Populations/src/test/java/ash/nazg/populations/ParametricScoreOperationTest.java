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

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class ParametricScoreOperationTest {
    @Test
    public void calculatePostcodeTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/test.parametricScore.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> dataset = (JavaRDD<Text>) ret.get("scores");

            assertEquals(5, dataset.count());

            List<Text> sample = dataset.takeOrdered(5);

            assertEquals(
                    "59e7074894d2e,code-1,10.00049",
                    sample.get(0).toString().substring(0, 29)
            );

            assertEquals(
                    "59e7074894d84,code-4,12.00039",
                    sample.get(1).toString().substring(0, 29)
            );

            assertEquals(
                    "59e7074894dbb,code-4,7.00024",
                    sample.get(2).toString().substring(0, 28)
            );

            assertEquals(
                    "59e7074894df0,code-2,8.00029",
                    sample.get(3).toString().substring(0, 28)
            );

            assertEquals(
                    "59e7074894e26,code-5,12.00039",
                    sample.get(4).toString().substring(0, 29)
            );
        }
    }
}
