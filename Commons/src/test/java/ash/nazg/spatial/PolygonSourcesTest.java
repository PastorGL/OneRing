/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial;

import ash.nazg.spark.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import org.locationtech.jts.geom.Polygon;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PolygonSourcesTest {
    @Test
    public void polygonJsonSourceTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.json.polygons.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Polygon> rddS = (JavaRDD<Polygon>) ret.get("poi");
            assertEquals(
                    1,
                    rddS.count()
            );
        }
    }

    @Test
    public void polygonH3SourceTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.h3.polygons.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Polygon> rddS = (JavaRDD<Polygon>) ret.get("polygons");
            assertEquals(
                    10,
                    rddS.count()
            );
        }
    }
}
