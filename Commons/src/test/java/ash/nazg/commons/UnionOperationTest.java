package ash.nazg.commons;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class UnionOperationTest {
    @Test
    public void unionTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.union.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            long parts = 0L;
            long all = 0L;
            JavaRDD<Text> rdd = (JavaRDD<Text>) ret.get("one");
            parts += rdd.getNumPartitions();
            all += rdd.count();
            rdd = (JavaRDD<Text>) ret.get("one_off");
            parts += rdd.getNumPartitions();
            all += rdd.count();
            rdd = (JavaRDD<Text>) ret.get("another");
            parts += rdd.getNumPartitions();
            all += rdd.count();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("union");

            assertEquals(
                    parts,
                    resultRDD.getNumPartitions()
            );
            assertEquals(
                    all,
                    resultRDD.count()
            );
        }
    }

    @Test
    public void unionSpecTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test2.union.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("union_xor");

            assertEquals(
                    2,
                    resultRDD.count()
            );

            resultRDD = (JavaRDD<Text>) ret.get("union_and");
            assertEquals(
                    257,
                    resultRDD.count()
            );
        }
    }
}
