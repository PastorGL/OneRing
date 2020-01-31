/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.proximity;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AreaCoversTest {
    @Test
    public void areaCoversTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/config.covers.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("output");

            Assert.assertEquals(45, resultRDD.count());

        }
    }

    @Test
    public void areaFilterTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/test.config.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("output");

            assertEquals(718, resultRDD.count());

        }
    }
}
