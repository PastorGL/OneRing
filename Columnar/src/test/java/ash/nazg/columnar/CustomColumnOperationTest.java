package ash.nazg.columnar;

import ash.nazg.spark.TestRunner;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CustomColumnOperationTest {

    @Test
    public void customColumnTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.customcolumn.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("with_column");

            assertEquals(
                    28,
                    resultRDD.count()
            );

            List<Text> list = ret.get("with_column").collect();

            long count = list.stream().filter(text -> {
                CSVParser parser = new CSVParserBuilder().withSeparator(',').build();
                String[] row = new String[0];
                try {
                    row = parser.parseLine(text.toString());
                } catch (IOException e) {
                    fail();
                }
                return row[row.length - 2].equals("foo bar");
            }).count();

            assertEquals(
                    28,
                    count
            );

        }
    }
}
