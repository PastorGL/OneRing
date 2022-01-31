/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import scala.Tuple2;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class KeyedMathOperationTest {
    @Test
    @SuppressWarnings("unchecked")
    public void keyedMathTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.keyedMath.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            Text cd27220b = new Text("cd27220b-11e9-4d00-b914-eb567d4df6e7");
            Text c7e5a6f9 = new Text("c7e5a6f9-ca03-4554-a046-541ff46cd88b");

            Map<Text, Double> res = ((JavaPairRDD)ret.get("sum")).collectAsMap();
            assertEquals(0.D - 15, res.get(cd27220b), 0.D);
            assertNotEquals(0.D - 15, res.get(c7e5a6f9), 0.D);

            res = ((JavaPairRDD)ret.get("mean")).collectAsMap();
            assertEquals(0.D, res.get(cd27220b), 0.D);
            assertNotEquals(0.D, res.get(c7e5a6f9), 0.D);

            res = ((JavaPairRDD)ret.get("root_mean")).collectAsMap();
            assertEquals(222.4748765D, res.get(cd27220b), 1E-7D);
            assertNotEquals(0.D, res.get(c7e5a6f9), 0.D);

            res = ((JavaPairRDD)ret.get("min")).collectAsMap();
            assertEquals(0.D, res.get(cd27220b), 0.D);
            assertEquals(0.D, res.get(c7e5a6f9), 0.D);

            res = ((JavaPairRDD)ret.get("max")).collectAsMap();
            assertEquals(0.D, res.get(cd27220b), 0.D);
            assertEquals(27.5995511D, res.get(c7e5a6f9), 1E-6D);

            res = ((JavaPairRDD)ret.get("mul")).collectAsMap();
            assertEquals(20196938.49, res.get(cd27220b), 1E-2D);
            assertNotEquals(0.D, res.get(c7e5a6f9), 0.D);

            res = ((JavaPairRDD)ret.get("mul")).collectAsMap();
            assertEquals(20196938.49, res.get(cd27220b), 1E-2D);
            assertNotEquals(0.D, res.get(c7e5a6f9), 0.D);

            res = ((JavaPairRDD)ret.get("median")).collectAsMap();
            assertEquals(280.D, res.get(cd27220b), 0.D);
            assertEquals(280.D, res.get(c7e5a6f9), 0.D);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void keyedMinimaxTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.keyedMinimax.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            Text cd27220b = new Text("cd27220b-11e9-4d00-b914-eb567d4df6e7");
            Text c7e5a6f9 = new Text("c7e5a6f9-ca03-4554-a046-541ff46cd88b");

            Map<Text, Double> res = ((JavaPairRDD<Text, Text>) ret.get("min"))
                    .mapToPair(t-> new Tuple2<>(t._1, Double.parseDouble(t._2.toString().split("\t")[1])))
                    .collectAsMap();
            assertEquals(0.D, res.get(cd27220b), 0.D);
            assertEquals(0.D, res.get(c7e5a6f9), 0.D);

            res = ((JavaPairRDD<Text, Text>)ret.get("max"))
                    .mapToPair(t-> new Tuple2<>(t._1, Double.parseDouble(t._2.toString().split("\t")[1])))
                    .collectAsMap();
            assertEquals(0.D, res.get(cd27220b), 0.D);
            assertEquals(27.5995511D, res.get(c7e5a6f9), 1E-6D);
        }
    }
}
