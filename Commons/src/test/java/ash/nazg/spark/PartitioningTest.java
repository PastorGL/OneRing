/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spark;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PartitioningTest {
    @Test
    public void partitionTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.partition.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertEquals(
                    16,
                    rddS.getNumPartitions()
            );

            rddS = (JavaRDD<Text>) ret.get("signals1");
            assertEquals(
                    1,
                    rddS.getNumPartitions()
            );

            rddS = (JavaRDD<Text>) ret.get("signals2");
            assertEquals(
                    8,
                    rddS.getNumPartitions()
            );

            rddS = (JavaRDD<Text>) ret.get("signals3");
            assertEquals(
                    2,
                    rddS.getNumPartitions()
            );

            rddS = (JavaRDD<Text>) ret.get("signals4");
            assertEquals(
                    2,
                    rddS.getNumPartitions()
            );
        }
    }
}
