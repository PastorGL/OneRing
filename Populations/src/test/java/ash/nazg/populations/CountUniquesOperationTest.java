/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.populations;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;
import scala.Tuple2;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CountUniquesOperationTest {
    @Test
    public void countUniquesTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/test.countUniques.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> dataset = (JavaRDD<Text>) ret.get("result");

            Map<String, Long> resMap = dataset.mapToPair(t -> {
                String[] s = t.toString().split("\t", 2);

                return new Tuple2<>(s[0], new Long(s[1]));
            }).collectAsMap();

            assertEquals(10L, resMap.get("gid-all").longValue());
            assertEquals(1L, resMap.get("gid-onlyone").longValue());
            assertEquals(6L, resMap.get("gid-some").longValue());

        }
    }
}
