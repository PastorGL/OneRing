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


public class DwellTimeOperationTest {
    @Test
    public void dwellTimeTest() {
        try (TestRunner underTest = new TestRunner("/configs/test.dwellTime.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            Map<String, Columnar> resMap = ((JavaPairRDD<String, Columnar>) ret.get("result")).collectAsMap();

            assertEquals(0.36666666666, resMap.get("cell1").asDouble("_dwelltime"), 1E-06);
            assertEquals(0.3D, resMap.get("cell2").asDouble("_dwelltime"), 1E-06);
            assertEquals(0.75D, resMap.get("cell3").asDouble("_dwelltime"), 1E-06);
        }
    }
}
