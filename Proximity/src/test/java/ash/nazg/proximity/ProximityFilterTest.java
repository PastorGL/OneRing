package ash.nazg.proximity;

import ash.nazg.commons.TextUtil;
import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ProximityFilterTest {

    @Test
    public void proximityFilterTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/config.proximity.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("output");

            assertEquals(44, resultRDD.count());

            List<Text> sample = resultRDD.collect();

            for (Text s : sample) {
                double dist = new Double(String.valueOf(TextUtil.column(s, ",", 5)));
                assertTrue(dist <= 30000.D);
            }

        }
    }

}
