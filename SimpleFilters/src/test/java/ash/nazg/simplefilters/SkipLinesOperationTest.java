/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SkipLinesOperationTest {
    @Test
    public void skipValueTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.skipValue.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<Text> res = ((JavaRDD<Text>) ret.get("filtered")).collect();

            assertFalse(res.isEmpty());
            for (Object o : res) {
                assertNotEquals("ts,group,uid,lat,lon", String.valueOf(o));
            }
        }
    }

    @Test
    public void skipPatternTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.skipPattern.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<Text> res = ((JavaRDD<Text>) ret.get("filtered")).collect();

            assertFalse(res.isEmpty());
            for (Object o : res) {
                assertNotEquals("ts,group,uid,lat,lon", String.valueOf(o));
                assertFalse(Double.parseDouble(String.valueOf(o).split(",", 5)[3]) < 32.D);
            }
        }
    }

    @Test
    public void skipPatternReverseTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.skipPatternReverse.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<Text> res = ((JavaRDD<Text>) ret.get("filtered")).collect();

            assertFalse(res.isEmpty());
            for (Object o : res) {
                assertTrue(Double.parseDouble(String.valueOf(o).split(",", 5)[3]) < 32.D);
            }
        }
    }
}
