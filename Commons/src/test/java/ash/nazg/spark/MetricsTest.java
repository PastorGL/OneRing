package ash.nazg.spark;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricsTest {
    @Test
    public void metricsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/metrics.properties")) {
            Map<String, JavaRDDLike> output = underTest.go();

            JavaRDD<Text> metricsRdd = (JavaRDD<Text>) output.get("_metrics");

            Map<String, String> metrics = metricsRdd.collect().stream().map(t -> {
                String[] v = String.valueOf(t).split(",");

                return new AbstractMap.SimpleEntry<>(v[0], v[4]);
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            assertTrue(metrics.containsKey("signals"));
            assertEquals("2", metrics.get("signals"));
            assertTrue(metrics.containsKey("nop1"));
            assertTrue(metrics.containsKey("nop2"));
            assertTrue(metrics.containsKey("result"));
            assertEquals("2", metrics.get("result"));
        }
    }
}
