package ash.nazg.spark;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ControlFlowComplexTest {
    @Test
    public void ifNestedIterTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.IF-nested-ITER.properties", "/controlFlow/vars.properties")) {

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

            rddS = (JavaRDD<Text>) ret.get("unexpected");
            assertNull(
                    rddS
            );
        }
    }

    @Test
    public void iterNestedIterTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.ITER-nested-ITER.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-a-1");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-a-2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-a-3");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-b-1");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-b-2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-b-3");
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
    public void nightmareModeTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.nightmare-mode.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yup");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-1");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-2");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-3");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-a");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("signals-b");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("nope");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("none");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("nay");
            assertNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yep");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yap");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yop");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("yass");
            assertNotNull(
                    rddS
            );
        }
    }
}
