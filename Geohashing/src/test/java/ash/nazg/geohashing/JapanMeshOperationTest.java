/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.geohashing;

import ash.nazg.spark.TestRunner;
import ash.nazg.geohashing.functions.JapanMeshFunction;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JapanMeshOperationTest {

    @Test
    public void japanMeshTest() throws Exception {
        try (TestRunner underTest = new TestRunner("/test.japanmesh.properties")) {

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
            //expected output: 32.21948,131.71303,%02d%02d%d%d%d%d%d%d,00000000-0000-0000-0000-000000000000,1474303143
            Pattern p = Pattern.compile("\\d+\\.\\d+,\\d+\\.\\d+,\\d{10},.{36},\\d+");

            for (Text l : list) {
                if (!p.matcher(l.toString()).matches()) {
                    fail();
                }
            }

        }
    }

    @Test
    public void japanMeshAlgoTest() {
        JapanMeshFunction japanMesh = new JapanMeshFunction(5);

        assertEquals("5737144744", japanMesh.getHash(38.12403, 137.59835));

        japanMesh = new JapanMeshFunction(6);
        assertEquals("49323444444", japanMesh.getHash(32.95776, 132.56197));
        assertEquals("39270594423", japanMesh.getHash(26.08107, 127.68476));

        //border point
        japanMesh = new JapanMeshFunction(3);
        assertEquals("64414277", japanMesh.getHash(43.05833334, 141.33750000));
    }
}
