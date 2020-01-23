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
}
