/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapJoinTest {
    @Test
    public void mapJoinTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.mapjoin.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaPairRDD<Text, Text> left = (JavaPairRDD<Text, Text>) ret.get("left_pair");
            assertEquals(
                    6,
                    left.count()
            );

            JavaPairRDD<Text, Text> right = (JavaPairRDD<Text, Text>) ret.get("right_pair");
            assertEquals(
                    6,
                    right.count()
            );

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("joined");

            assertEquals(
                    12,
                    resultRDD.count()
            );
        }
    }


    @Test
    public void mapDifferentJoinsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.mapjoin.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("joined_left");

            assertEquals(
                    6,
                    resultRDD.count()
            );

            resultRDD = (JavaRDD<Text>) ret.get("joined_right");

            assertEquals(
                    4,
                    resultRDD.count()
            );

            resultRDD = (JavaRDD<Text>) ret.get("joined_outer");

            assertEquals(
                    8,
                    resultRDD.count()
            );
        }
    }
}
