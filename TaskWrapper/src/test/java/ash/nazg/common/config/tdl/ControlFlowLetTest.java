/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.common.config.tdl;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class ControlFlowLetTest {
    @Test
    public void letTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.LET.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("vars");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-YES");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-AB");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-LIST");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void letPairTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.LET-pair.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("vars_source");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-YES");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-AB");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("out-LIST");
            assertNotNull(
                    rddS
            );
        }
    }
}
