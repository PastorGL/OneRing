/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapCountTest {
    @Test
    public void mapCountTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.mapcount.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaPairRDD<Text, Text> left = (JavaPairRDD<Text, Text>) ret.get("pair");
            assertEquals(
                    6,
                    left.count()
            );

            Map<Text, Long> result = ((JavaPairRDD<Text, Long>) ret.get("counted")).collectAsMap();

            assertEquals(
                    3,
                    result.size()
            );

            for (Long l : result.values()) {
                assertEquals(2L, l.longValue());
            }
        }
    }
}
