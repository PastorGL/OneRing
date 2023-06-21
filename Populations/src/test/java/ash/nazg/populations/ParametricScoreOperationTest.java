/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations;

import ash.nazg.data.Columnar;
import ash.nazg.scripting.TestRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;


public class ParametricScoreOperationTest {
    @Test
    public void calculatePostcodeTest() {
        try (TestRunner underTest = new TestRunner("/configs/test.parametricScore.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            Map<String, Columnar> dataset = ((JavaPairRDD<String, Columnar>) ret.get("scores")).collectAsMap();

            assertEquals(
                    "code-1",
                    dataset.get("59e7074894d2e").asString("_value_1")
            );
            assertEquals(
                    "10.00049",
                    dataset.get("59e7074894d2e").asString("_score_1")
            );

            assertEquals(
                    "code-5",
                    dataset.get("59e7074894e26").asString("_value_1")
            );
            assertEquals(
                    "12.00039",
                    dataset.get("59e7074894e26").asString("_score_1")
            );
        }
    }
}
