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

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SplitByColumnOperationTest {
    @Test
    public void splitByColumnTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.splitByColumn.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            List<Text> splitValues = ((JavaRDD<Text>) ret.get("split_values")).collect();

            assertEquals(
                    5,
                    splitValues.size()
            );

            CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
            for (Text split : splitValues) {
                String splitStr = String.valueOf(split);

                List list = ((JavaRDD<Text>) ret.get("pref_" + splitStr + "_suff")).collect();

                for (Object line : list) {
                    String[] row = parser.parseLine(String.valueOf(line));

                    assertEquals(splitStr, row[1]);
                }
            }

        }
    }
}
