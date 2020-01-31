/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import scala.Tuple2;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WeightedSumOperationTest {
    @Test
    public void weightedSumTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.weightedsum.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("aggregated");

            assertEquals(
                    9,
                    resultRDD.count()
            );

            Map<String, Double> resultMap = resultRDD.mapToPair(t -> {
                String[] row = t.toString().split(",");
                return new Tuple2<>(row[0] + "," + row[1], new Double(row[2]));
            }).collectAsMap();

            assertEquals(
                    51.82622694208983,
                    resultMap.get("288,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );

            assertEquals(
                    79.27389411735051,
                    resultMap.get("288,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );

            assertEquals(
                    205.42487681041123,
                    resultMap.get("288,6a01c5de-4563-4afb-bf59-21f1f75e9eab"),
                    1.E-6D
            );

        }
    }

    @Test
    public void weightedSumWildcardTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test2.weightedsum.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("aggregated");

            assertEquals(
                    9,
                    resultRDD.count()
            );

            Map<String, Double> resultMap = resultRDD.mapToPair(t -> {
                String[] row = t.toString().split(",");
                return new Tuple2<>(row[0] + "," + row[1], new Double(row[2]));
            }).collectAsMap();

            assertEquals(
                    142.87672116435095,
                    resultMap.get("280,c7e5a6f9-ca03-4554-a046-541ff46cd88b"),
                    1.E-6D
            );
            assertEquals(
                    61.909924858912355,
                    resultMap.get("237,cd27220b-11e9-4d00-b914-eb567d4df6e7"),
                    1.E-6D
            );
            assertEquals(
                    316.285881692825,
                    resultMap.get("280,6a01c5de-4563-4afb-bf59-21f1f75e9eab"),
                    1.E-6D
            );

        }
    }
}