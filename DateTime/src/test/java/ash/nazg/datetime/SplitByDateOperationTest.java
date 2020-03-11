/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.datetime;

import ash.nazg.spark.TestRunner;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SplitByDateOperationTest {
    @Test
    public void splitByDateTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.splitbydate.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            Pattern p = Pattern.compile("split_(\\d+)_(\\d+)");

            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();

            long expectedSplitCount = ret.get("splits").count();

            assertNotEquals(0, expectedSplitCount);

            Set<String> splits = new HashSet<>();
            long esc = 0;
            for (String ks : ret.keySet()) {
                Matcher m = p.matcher(ks);
                if (m.matches()) {
                    splits.add(ks);

                    esc++;

                    String ye = m.group(1);
                    String mo = m.group(2);

                    List<Text> result = ret.get(ks).collect();

                    assertNotEquals(0, result.size());

                    for (Text t : result) {
                        String[] ll = parser.parseLine(String.valueOf(t));

                        assertEquals(ye, ll[9]);
                        assertEquals(mo, ll[8]);
                    }
                }
            }

            assertEquals(expectedSplitCount, esc);

        }
    }
}
