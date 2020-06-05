package ash.nazg.common.config.tdl;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ControlFlowIfTest {
    @Test
    public void ifDefaultsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.IF-defaults.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void ifElseSetTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.IF-ELSE-set.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
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
    public void ifElseUnsetTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.IF-ELSE-unset.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
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
    public void ifNestedTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.IF-nested.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }

    @Test
    public void ifNestedElseTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.IF-nested-ELSE.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
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
    public void ifNoDefaultsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.IF-no-defaults.properties")) {

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
    public void ifSetTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/controlFlow/test.IF-set.properties", "/controlFlow/vars.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("signals");
            assertNotNull(
                    rddS
            );

            rddS = (JavaRDD<Text>) ret.get("expected");
            assertNotNull(
                    rddS
            );
        }
    }
}
