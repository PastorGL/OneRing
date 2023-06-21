/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.geohashing;

import ash.nazg.data.Columnar;
import ash.nazg.geohashing.functions.JapanMeshFunction;
import ash.nazg.scripting.TestRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JapanMeshOperationTest {
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

    @Test
    public void japanMeshTest() {
        try (TestRunner underTest = new TestRunner("/test.japanMesh.tdl")) {
            Map<String, JavaRDDLike> ret = underTest.go();

            List<Columnar> result = ((JavaRDD<Columnar>) ret.get("with_hash")).collect();
            assertEquals(
                    28,
                    result.size()
            );

            JapanMeshFunction japanMesh = new JapanMeshFunction(5);
            for (Columnar l : result) {
                if (!japanMesh.getHash(l.asDouble("lat"), l.asDouble("lon")).equals(l.asString("_hash"))) {
                    fail();
                }
            }
        }
    }
}
