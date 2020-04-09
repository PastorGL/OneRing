package ash.nazg.spark;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ControlFlowIterTest {
    @Test
    public void iterDefaultsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.ITER-defaults.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals1");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals3");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void iterElseSetTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.ITER-ELSE-set.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void iterElseUnsetTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.ITER-ELSE-unset.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void iterNoDefaultsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.ITER-no-defaults.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("unexpected");
            assertNull(
                    rddS
            );
        }
    }

    @Test
    public void iterSetTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.ITER-set.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals1");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals3");
            assertNotNull(
                    rddS
            );
        }
    }
}
