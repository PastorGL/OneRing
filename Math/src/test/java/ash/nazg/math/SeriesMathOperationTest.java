/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.math;

import ash.nazg.data.Columnar;
import ash.nazg.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class SeriesMathOperationTest {
    @Test
    public void seriesTest() {
        try (TestRunner underTest = new TestRunner("/test.seriesMath.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<Columnar> result = ((JavaRDD<Columnar>) ret.get("normalized")).collect();

            assertEquals(
                    6,
                    result.size()
            );

            Map<String, Double> resultMap = result.stream()
                    .collect(Collectors.toMap(t -> t.asString("catid") + "," + t.asString("userid"), t -> t.asDouble("_result")));

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

            result = ((JavaRDD<Columnar>) ret.get("stddev")).collect();

            assertEquals(
                    6,
                    result.size()
            );

            resultMap = result.stream()
                    .collect(Collectors.toMap(t -> t.asString("catid") + "," + t.asString("userid"), t -> t.asDouble("_result")));

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