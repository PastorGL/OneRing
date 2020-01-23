package ash.nazg.proximity;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

public class FullJoinTest {
    @Test
    public void fullJoinTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/grid_join/config.fulljoin.properties")) {

            JavaRDD<Text> dataset = (JavaRDD<Text>) underTest.go().get("signals_output");

            Assert.assertEquals(722, dataset.count());

        }
    }
}
