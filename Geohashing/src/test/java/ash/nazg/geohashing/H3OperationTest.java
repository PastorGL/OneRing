package ash.nazg.geohashing;

import ash.nazg.spark.TestRunner;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class H3OperationTest {

    @Test
    public void h3Test() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.h3.properties")) {

            Map<String, JavaRDDLike> ret = underTest.go();

            JavaRDD<Text> resultRDD = (JavaRDD<Text>) ret.get("with_hash");

            assertEquals(
                    28,
                    resultRDD.count()
            );

            List<Text> list = ret.get("with_hash").collect();

            //input: 1474303143,1099,"00000000-0000-0000-0000-000000000000","32.21948","131.71303"
            //columns: ts,_,userid,lat,lon
            //converting to
            //def: signals.lat,signals.lon,_hash,signals.userid,signals.ts
            //expected output: 32.21948,131.71303,881e241153fffff,00000000-0000-0000-0000-000000000000,1474303143
            Pattern p = Pattern.compile("\\d+\\.\\d+,\\d+\\.\\d+,[0-9a-f]{15},.{36},\\d+");

            for (Text l : list) {
                if (!p.matcher(l.toString()).matches()) {
                    fail();
                }
            }

        }
    }
}
