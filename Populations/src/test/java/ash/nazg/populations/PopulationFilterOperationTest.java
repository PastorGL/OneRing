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

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PopulationFilterOperationTest {
    @Test
    public void populationFilterTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/test.populationfilter.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("filtered");

            resultRDD.takeOrdered(5).forEach(System.out::println);

            assertEquals(
                    163,
                    resultRDD.count()
            );

        }
    }
}
