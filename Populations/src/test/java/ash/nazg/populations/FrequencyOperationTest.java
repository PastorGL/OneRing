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


public class FrequencyOperationTest {
    @Test
    public void frequencyTest() {
        try (TestRunner underTest = new TestRunner("/configs/test.frequency.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            Map<String, Columnar> resMap = ((JavaPairRDD<String, Columnar>) ret.get("result")).collectAsMap();

            assertEquals(0.25D, resMap.get("2").asDouble("_frequency"), 1E-06);
            assertEquals(1.D / 3.D, resMap.get("4").asDouble("_frequency"), 1E-06);
        }
    }
}
