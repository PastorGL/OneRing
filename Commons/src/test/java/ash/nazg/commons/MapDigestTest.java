/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapDigestTest {
    @Test
    public void mapDigestTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.mapdigest.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            Map<Text, Text> left = ((JavaPairRDD<Text, Text>) ret.get("pair")).collectAsMap();

            MessageDigest md5 = MessageDigest.getInstance("MD5");

            for (Map.Entry<Text, Text> e : left.entrySet()) {
                String[] cols = e.getValue().toString().split(",");

                md5.update(cols[0].getBytes());
                md5.update((byte) 0);
                md5.update(cols[1].getBytes());
                md5.update((byte) 0);
                md5.update(cols[2].getBytes());

                assertEquals(DatatypeConverter.printHexBinary(md5.digest()), e.getKey().toString());
            }
        }
    }
}
