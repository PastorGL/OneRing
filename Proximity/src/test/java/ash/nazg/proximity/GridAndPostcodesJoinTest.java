/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.proximity;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

public class GridAndPostcodesJoinTest {

    @Test
    public void partial1JoinTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/configs/grid_join/config.grid_and_postcodes_join.properties")) {

            JavaRDD<Text> dataset = (JavaRDD<Text>) underTest.go().get("signals_output");

            Assert.assertEquals(765, dataset.count());

        }
    }
}
