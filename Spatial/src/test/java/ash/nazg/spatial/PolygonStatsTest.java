/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial;

import ash.nazg.data.spatial.PolygonEx;
import ash.nazg.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class PolygonStatsTest {
    @Test
    public void polygonStatsTest() {
        try (TestRunner underTest = new TestRunner("/test.polygonStats.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PolygonEx> rddS = (JavaRDD<PolygonEx>) ret.get("stats");
            Assert.assertEquals(14, rddS.count());

            List<PolygonEx> datas = rddS
                    .collect();

            for (PolygonEx data : datas) {
                Assert.assertTrue(data.asDouble("_perimeter") > 0.D);
                Assert.assertTrue(data.asDouble("_area") > 0.D);
            }
        }
    }
}
