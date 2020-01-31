/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.dist;

import ash.nazg.config.InvalidConfigValueException;
import org.junit.Test;
import scala.Tuple2;

import java.util.Map;

import static org.junit.Assert.*;

public class DistUtilTests {
    @Test
    public void splitTestValid() throws InvalidConfigValueException {
        Map<String, Tuple2<String, String>> splits = DistUtils.globCSVtoRegexMap(
                "s3://mama/asdfasf/{sdf,sdfsdf,sdsdf{sdfsdf,sdf}}" +
                        ",s3://sdfsdf/sdfs/sdf" +
                        ",s3://nnn/sad/\\{sdfsdf??" +
                        ",s3://doh/woh/sdfsdf\\}.*" +
                        ",s3://222/x/much\\\\sre?" +
                        ",file:/D:/qwer/ty/[!a-d].*" +
                        ",hdfs:///not/skipped/path*"
        );

        assertEquals(7, splits.size());

        Tuple2<String, String> s1 = splits.get("asdfasf");
        assertEquals("mama/asdfasf", s1._1);
        assertEquals(".*/(asdfasf)/(?:sdf|sdfsdf|sdsdf(?:sdfsdf|sdf)).*", s1._2);

        Tuple2<String, String> s2 = splits.get("sdf");
        assertEquals("sdfsdf/sdfs/sdf", s2._1);
        assertEquals(".*/(sdf).*", s2._2);

        Tuple2<String, String> s3 = splits.get("sad");
        assertEquals("nnn/sad", s3._1);
        assertEquals(".*/(sad)/\\{sdfsdf...*", s3._2);

        Tuple2<String, String> s4 = splits.get("woh");
        assertEquals("doh/woh", s4._1);
        assertEquals(".*/(woh)/sdfsdf\\}..*.*", s4._2);

        Tuple2<String, String> s5 = splits.get("x");
        assertEquals("222/x", s5._1);
        assertEquals(".*/(x)/much\\\\sre..*", s5._2);

        Tuple2<String, String> s6 = splits.get("ty");
        assertEquals("D:/qwer/ty", s6._1);
        assertEquals(".*/(ty)/[^a-d]..*.*", s6._2);

        Tuple2<String, String> s7 = splits.get("skipped");
        assertEquals("not/skipped", s7._1);
        assertEquals(".*/(skipped)/path.*.*", s7._2);
    }

    @Test
    public void splitTestInvalid() {
        try {
            DistUtils.globCSVtoRegexMap(
                    "s3://mama/xx{sdf,sdfsdf,sdsdf{sdfsdf,sdf}}"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern 's3://mama/xx{sdf,sdfsdf,sdsdf{sdfsdf,sdf}}' has no valid grouping candidate part in the path", e.getMessage());
        }

        try {
            DistUtils.globCSVtoRegexMap(
                    "s3://sdfsdf"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern 's3://sdfsdf' must have protocol specification and its first path part must be not a grouping candidate", e.getMessage());
        }

        try {
            DistUtils.globCSVtoRegexMap(
                    "/no/protocol/part"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern '/no/protocol/part' must have protocol specification and its first path part must be not a grouping candidate", e.getMessage());
        }

        try {
            DistUtils.globCSVtoRegexMap(
                    "s3://same/this/*.csv" +
                            ",s3://same/this/*.tsv"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern 's3://same/this/*.tsv' tries to overwrite another glob pattern with grouping part 'this'", e.getMessage());
        }

        try {
            DistUtils.globCSVtoRegexMap(
                    "file://shoot"
            );

            fail();
        } catch (InvalidConfigValueException e) {
            assertEquals("Glob pattern 'file://shoot' must have protocol specification and its first path part must be not a grouping candidate", e.getMessage());
        }
    }
}
