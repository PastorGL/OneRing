package ash.nazg.commons;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DistinctOperationTest {

    @Test
    public void distinctTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.distinct.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("distinct");

            assertEquals(
                    60,
                    resultRDD.count()
            );

        }
    }
}
