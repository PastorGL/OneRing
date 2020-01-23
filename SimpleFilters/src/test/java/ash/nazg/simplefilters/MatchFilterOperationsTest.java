package ash.nazg.simplefilters;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MatchFilterOperationsTest {
    @Test
    public void exactMatchTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.exactMatch.properties")) {

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
    public void listMatchTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/config.listMatch.properties")) {

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
}
