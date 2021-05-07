/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.hadoop;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.storage.hadoop.FileStorage;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FileStorageTests {
    @Test
    public void srcDestGroupTest() throws InvalidConfigValueException {
        List<Tuple2<String, String>> splits = FileStorage.srcDestGroup(
                "s3://mama/asdfasf/{sdf,sdfsdf,sdsdf{sdfsdf,sdf}}" +
                        ",s3://sdfsdf/sdfs/sdf" +
                        ",s3://nnn/sad/\\{sdfsdf??" +
                        ",s3://doh/woh/sdfsdf\\}.*" +
                        ",s3://222/x/much\\\\sre?" +
                        ",file:/D:/qwer/ty/[!a-d].*" +
                        ",hdfs:///not/skipped/path*" +
                        ",stor:/path/path2/{10/02/2020,11/02/2020}"
        );

        assertEquals(8, splits.size());

        Tuple2<String, String> s1 = splits.get(0);
        assertEquals("s3://mama/asdfasf", s1._1);
        assertEquals(".*/(asdfasf)/(?:sdf|sdfsdf|sdsdf(?:sdfsdf|sdf)).*", s1._2);

        Tuple2<String, String> s2 = splits.get(1);
        assertEquals("s3://sdfsdf/sdfs/sdf", s2._1);
        assertEquals(".*/(sdf).*", s2._2);

        Tuple2<String, String> s3 = splits.get(2);
        assertEquals("s3://nnn/sad", s3._1);
        assertEquals(".*/(sad)/\\{sdfsdf...*", s3._2);

        Tuple2<String, String> s4 = splits.get(3);
        assertEquals("s3://doh/woh", s4._1);
        assertEquals(".*/(woh)/sdfsdf\\}..*.*", s4._2);

        Tuple2<String, String> s5 = splits.get(4);
        assertEquals("s3://222/x", s5._1);
        assertEquals(".*/(x)/much\\\\sre..*", s5._2);

        Tuple2<String, String> s6 = splits.get(5);
        assertEquals("file:/D:/qwer/ty", s6._1);
        assertEquals(".*/(ty)/[^a-d]..*.*", s6._2);

        Tuple2<String, String> s7 = splits.get(6);
        assertEquals("hdfs:///not/skipped", s7._1);
        assertEquals(".*/(skipped)/path.*.*", s7._2);

        Tuple2<String, String> s8 = splits.get(7);
        assertEquals("stor:/path/path2", s8._1);
        assertEquals(".*/(path2)/(?:10/02/2020|11/02/2020).*", s8._2);
    }

    @Test
    public void splitTestInvalid() {
        try {
            FileStorage.srcDestGroup(
                    "s3://mama/xx{sdf,sdfsdf,sdsdf{sdfsdf,sdf}}"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern 's3://mama/xx{sdf,sdfsdf,sdsdf{sdfsdf,sdf}}' has no valid grouping candidate part in the path", e.getMessage());
        }

        try {
            FileStorage.srcDestGroup(
                    "s3://sdfsdf"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern 's3://sdfsdf' must have protocol specification and its first path part must be not a grouping candidate", e.getMessage());
        }

        try {
            FileStorage.srcDestGroup(
                    "/no/protocol/part"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern '/no/protocol/part' must have protocol specification and its first path part must be not a grouping candidate", e.getMessage());
        }

        try {
            FileStorage.srcDestGroup(
                    "file://shoot/[me"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern 'file://shoot/[me' contains unbalances range [] or braces {} definition", e.getMessage());
        }

        try {
            FileStorage.srcDestGroup(
                    "file://shoot/{me,too"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern 'file://shoot/{me,too' contains unbalances range [] or braces {} definition", e.getMessage());
        }
    }
}
