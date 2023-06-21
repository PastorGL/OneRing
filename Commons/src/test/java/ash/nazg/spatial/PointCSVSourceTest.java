/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial;

import ash.nazg.data.spatial.PointEx;
import ash.nazg.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PointCSVSourceTest {
    @Test
    public void sourceTest() {
        try (TestRunner underTest = new TestRunner("/test.textToPoint.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PointEx> rddS = (JavaRDD<PointEx>) ret.get("source");
            assertEquals(
                    12,
                    rddS.count()
            );

            List<Double> radii = rddS
                    .map(PointEx::getRadius)
                    .collect();

            for (Double radius : radii) {
                assertNotEquals(300.D, radius, 0.D);
            }
        }
    }
}
