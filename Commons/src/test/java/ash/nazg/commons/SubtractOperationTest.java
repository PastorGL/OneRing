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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class SubtractOperationTest {

    @Test
    public void subtractPlainTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.subtract.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<String> subtrahendLines = ret.get("subtrahend").map(String::valueOf).collect();
            List<String> expectedDiff = ((JavaRDD<Object>) ret.get("minuend")).map(String::valueOf).collect().stream()
                    .filter(s -> !subtrahendLines.contains(s))
                    .collect(Collectors.toList());
            Collections.sort(expectedDiff);

            List<String> diff = new ArrayList<>(ret.get("difference").map(String::valueOf).collect());
            Collections.sort(diff);

            assertEquals(expectedDiff, diff);
            assertFalse(ret.get("subtrahend").map(String::valueOf).collect().stream().anyMatch(diff::contains));
            assertTrue(ret.get("minuend").map(String::valueOf).collect().containsAll(diff));
        }
    }

    @Test
    public void subtractPairKeysTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test2.subtract.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<String> subtrahendLines = ((JavaPairRDD<Object,Object>) ret.get("subtrahend_p")).keys().map(String::valueOf).collect();
            List<String> expectedDiff = ((JavaPairRDD<Object,Object>) ret.get("minuend_p")).keys().map(String::valueOf).collect().stream()
                    .filter(s -> !subtrahendLines.contains(s))
                    .collect(Collectors.toList());

            List<String> diff = new ArrayList<>(((JavaPairRDD<Text, Text>) ret.get("difference")).keys().map(String::valueOf).collect());

            assertEquals(expectedDiff, diff);
            assertFalse(((JavaPairRDD<Text, Text>) ret.get("subtrahend_p")).keys().map(String::valueOf).collect().stream().anyMatch(diff::contains));
            assertTrue(((JavaPairRDD<Text, Text>) ret.get("minuend_p")).keys().map(String::valueOf).collect().containsAll(diff));
        }
    }

    @Test
    public void subtractColumnTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test3.subtract.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<String> subtrahendLines = ((JavaRDD<Object>) ret.get("subtrahend")).map(String::valueOf).collect().stream()
                    .map(s -> s.split(",", 2)[1])
                    .collect(Collectors.toList());
            List<String> expectedDiff = ((JavaRDD<Object>) ret.get("minuend")).map(String::valueOf).collect().stream()
                    .filter(s -> !subtrahendLines.contains(s))
                    .collect(Collectors.toList());

            List<String> diff = new ArrayList<>(ret.get("difference").map(String::valueOf).collect());

            assertEquals(expectedDiff, diff);
            assertFalse(((JavaPairRDD<Text, Text>) ret.get("subtrahend_p")).keys().map(String::valueOf).collect().stream().anyMatch(diff::contains));
            assertTrue(ret.get("minuend").map(String::valueOf).collect().containsAll(diff));
        }
    }
}
