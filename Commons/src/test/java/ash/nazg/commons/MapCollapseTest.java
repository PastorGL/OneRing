package ash.nazg.commons;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapCollapseTest {
    @Test
    public void mapCollapseTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.mapcollapse.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaPairRDD<Text, Text> left = (JavaPairRDD<Text, Text>) ret.get("pair");
            assertEquals(
                    6,
                    left.count()
            );

            List<String> result = ((JavaRDD<Text>) ret.get("collapsed")).map(String::valueOf).collect();

            assertEquals(
                    6,
                    result.size()
            );

            for (String r : result) {
                assertEquals('|', r.charAt(3));
            }
        }
    }

    @Test
    public void mapCollapseCustomTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.mapcollapse.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            List<String> result = ((JavaRDD<Text>) ret.get("custom")).map(String::valueOf).collect();

            assertEquals(
                    6,
                    result.size()
            );

            for (String r : result) {
                String[] split = r.split("\\|");
                assertEquals(3, split.length);
            }
        }
    }
}
