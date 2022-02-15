/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class ColumnarToolboxTest {
    @Test
    public void columnarToolboxTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.columnarToolbox.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("ret1");

            assertEquals(11, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret2");

            assertEquals(4, rddS.count());
            for (Text data : rddS.collect()) {
                String[] c = data.toString().split("\t");
                double acc = Double.parseDouble(c[2]);
                assertTrue(acc >= 15.D);
                assertTrue(acc < 100.D);
            }

            rddS = (JavaRDD<Text>) ret.get("ret3");

            assertEquals(15, rddS.count());
            Pattern p = Pattern.compile(".+?non.*");
            for (Text data : rddS.collect()) {
                String[] c = data.toString().split("\t");
                assertTrue("e2e".equals(c[2]) || p.matcher(c[3]).matches());
            }

            rddS = (JavaRDD<Text>) ret.get("ret4");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret5");

            assertEquals(37, rddS.count());
            for (Text data : rddS.collect()) {
                String[] c = data.toString().split("\t");
                double acc = Double.parseDouble(c[3]);
                assertEquals(-24.02D, acc, 1E-03D);
                long cca = (long) Double.parseDouble(c[4]);
                assertEquals(100500L, cca);
                assertEquals("immediate", c[2]);
                assertEquals("null", c[5]);
            }

            rddS = (JavaRDD<Text>) ret.get("ret6");

            assertEquals(26, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret7");

            assertEquals(33, rddS.count());
            for (Text data : rddS.collect()) {
                String[] c = data.toString().split("\t");
                double acc = Double.parseDouble(c[2]);
                assertTrue(acc < 15.D || acc >= 100.D);
            }

            rddS = (JavaRDD<Text>) ret.get("ret8");

            assertEquals(22, rddS.count());
            for (Text data : rddS.collect()) {
                String[] c = data.toString().split("\t");
                assertFalse(!"e2e".equals(c[12]) && p.matcher(c[14]).matches());
            }

            rddS = (JavaRDD<Text>) ret.get("ret9");

            assertEquals(0, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret10");

            assertEquals(8, rddS.count());
            for (Text data : rddS.collect()) {
                int c = Integer.parseInt(data.toString());
                assertTrue(c >= 8);
                assertTrue(c <= 15);
            }

            rddS = (JavaRDD<Text>) ret.get("ret20");

            assertEquals(3, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret21");

            assertEquals(24, rddS.count());
        }
    }

    @Test
    public void selectByColumnTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.columnarToolbox2.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("ret11");

            assertEquals(9, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret12");

            assertEquals(28, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret13");

            assertEquals(35, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret14");

            assertEquals(0, rddS.count());
        }
    }

    @Test
    public void selectSubqueryTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.columnarSubquery.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("ret1");

            assertEquals(11, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret2");

            assertEquals(30, rddS.count());
        }
    }

    @Test
    public void selectUnionTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.columnarUnion.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("union");

            assertEquals(259, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("union_and");

            assertEquals(1, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("union_xor");

            assertEquals(2, rddS.count());
        }
    }

    @Test
    public void selectJoinTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.columnarJoin.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaPairRDD resultRDD = (JavaPairRDD) ret.get("joined");
            assertEquals(
                    12,
                    resultRDD.count()
            );

            resultRDD = (JavaPairRDD) ret.get("joined_left");
            assertEquals(
                    6,
                    resultRDD.count()
            );

            resultRDD = (JavaPairRDD) ret.get("joined_right");
            assertEquals(
                    4,
                    resultRDD.count()
            );

            resultRDD = (JavaPairRDD) ret.get("joined_outer");
            assertEquals(
                    8,
                    resultRDD.count()
            );
        }
    }

    @Test
    public void selectExpressionsTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.columnarToolbox3.properties")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> rddS = (JavaRDD<Text>) ret.get("ret1");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret2");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret2");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret3");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret4");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret5");

            assertEquals(37, rddS.count());

            rddS = (JavaRDD<Text>) ret.get("ret6");

            assertEquals(37, rddS.count());
        }
    }
}
