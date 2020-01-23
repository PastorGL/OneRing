package ash.nazg.math;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import scala.Tuple2;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SeriesMathOperationTest {

    @Test
    public void seriesTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.seriesMath.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("normalized");

            assertEquals(
                    6,
                    resultRDD.count()
            );

            Map<String, Double> resultMap = resultRDD.mapToPair(t -> {
                String[] row = t.toString().split(",");
                return new Tuple2<>(row[0] + "," + row[1], new Double(row[3]));
            }).collectAsMap();

            assertEquals(
                    81.38744830071273,
                    resultMap.get("288,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    100.0,
                    resultMap.get("280,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    3.120834596972309,
                    resultMap.get("237,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    52.979369624482665,
                    resultMap.get("288,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );
            assertEquals(
                    88.48815398118919,
                    resultMap.get("280,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );
            assertEquals(
                    67.44166429450938,
                    resultMap.get("237,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );

            resultRDD = (JavaRDD<Text>) ret.get("stddev");

            assertEquals(
                    6,
                    resultRDD.count()
            );

            resultMap = resultRDD.mapToPair(t -> {
                String[] row = t.toString().split(",");
                return new Tuple2<>(row[0] + "," + row[1], new Double(row[3]));
            }).collectAsMap();

            assertEquals(
                    0.49925794982803673,
                    resultMap.get("288,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    1.0867241826228398,
                    resultMap.get("280,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    -1.971063876485716,
                    resultMap.get("237,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    -0.3973835870495911,
                    resultMap.get("288,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );
            assertEquals(
                    0.7233768607484508,
                    resultMap.get("280,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );
            assertEquals(
                    0.05908847033597929,
                    resultMap.get("237,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );


        }
    }
}