/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.columnar;

import ash.nazg.spark.TestRunner;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DigestOperationTest {
    @Test
    public void digestTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.digest.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("with_digest");

            assertEquals(
                    28,
                    resultRDD.count()
            );

            List<Text> list = ret.get("with_digest").collect();

            MessageDigest md5 = MessageDigest.getInstance("MD5");
            MessageDigest sha = MessageDigest.getInstance("SHA");
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");

            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();

            for (Text line : list) {
                String[] row = parser.parseLine(String.valueOf(line));

                assertEquals(DatatypeConverter.printHexBinary(md5.digest(row[0].getBytes())), row[3]);
                assertEquals(DatatypeConverter.printHexBinary(sha.digest(row[1].getBytes())), row[4]);
                assertEquals(DatatypeConverter.printHexBinary(sha256.digest(row[2].getBytes())), row[5]);
                md5.update(row[1].getBytes());
                md5.update((byte) 0);
                md5.update(row[2].getBytes());
                assertEquals(DatatypeConverter.printHexBinary(md5.digest()), row[6]);
            }
        }
    }
}
