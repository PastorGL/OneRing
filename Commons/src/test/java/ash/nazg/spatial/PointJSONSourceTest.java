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

public class PointJSONSourceTest {
    @Test
    public void sourceTest() {
        try (TestRunner underTest = new TestRunner("/test.jsonToPoint.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PointEx> rddS = (JavaRDD<PointEx>) ret.get("source");
            assertEquals(
                    15,
                    rddS.count()
            );

            List<PointEx> points = rddS.collect();

            for (PointEx t : points) {
                Map<String, Object> data = (Map<String, Object>) t.getUserData();
                assertEquals(2, data.size());
                assertEquals(300.D, t.getRadius(), 0.D);
            }
        }
    }
}
