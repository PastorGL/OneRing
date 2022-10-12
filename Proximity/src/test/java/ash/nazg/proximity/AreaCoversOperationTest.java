/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.proximity;

import ash.nazg.data.spatial.PointEx;
import ash.nazg.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AreaCoversOperationTest {
    @Test
    public void areaCoversTest() {
        try (TestRunner underTest = new TestRunner("/configs/test.areaCovers.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PointEx> resultRDD = (JavaRDD<PointEx>) ret.get("joined");

            Assert.assertEquals(45, resultRDD.count());
        }
    }

    @Test
    public void areaFilterTest() {
        try (TestRunner underTest = new TestRunner("/configs/test2.areaCovers.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<PointEx> resultRDD = (JavaRDD<PointEx>) ret.get("filtered");

            assertEquals(718, resultRDD.count());

            resultRDD = (JavaRDD) ret.get("evicted");

            assertEquals(4, resultRDD.count());
        }
    }
}