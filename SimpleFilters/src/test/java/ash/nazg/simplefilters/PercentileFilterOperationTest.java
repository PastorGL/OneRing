package ash.nazg.simplefilters;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PercentileFilterOperationTest {

    @Test
    public void percentileFilterTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.percentileFilter.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> signalsRDD = (JavaRDD<Text>) ret.get("signals");
            assertEquals(
                    28,
                    signalsRDD.count()
            );

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("filtered");

            assertEquals(
                    21,
                    resultRDD.count()
            );

        }
    }

    @Test
    public void percentileTopTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.percentileFilter.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("filtered_top");

            assertEquals(
                    23,
                    resultRDD.count()
            );

        }
    }

    @Test
    public void percentileBottomTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.percentileFilter.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("filtered_bottom");

            assertEquals(
                    26,
                    resultRDD.count()
            );

        }
    }
}
