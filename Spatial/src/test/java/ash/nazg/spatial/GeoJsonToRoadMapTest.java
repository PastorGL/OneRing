/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial;

import ash.nazg.data.spatial.PolygonEx;
import ash.nazg.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GeoJsonToRoadMapTest {
    @Test
    public void polygonOutputsTest() {
        try (TestRunner underTest = new TestRunner("/test.geoJsonToRoadMap.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PolygonEx> rddS = (JavaRDD<PolygonEx>) ret.get("geometries");
            assertEquals(
                    44,
                    rddS.count()
            );
        }
    }
}
