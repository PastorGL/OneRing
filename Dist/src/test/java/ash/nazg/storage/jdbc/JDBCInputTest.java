/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.jdbc;

import ash.nazg.storage.StorageTestRunner;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class JDBCInputTest {
    @Ignore
    @Test
    public void testInput() throws Exception {
        try (StorageTestRunner underTest = new StorageTestRunner(false, "/config.JDBCInput.properties")) {
            Map<String, JavaRDDLike> res = underTest.go();

            long profiles_keyed = res.get("p2").count();
            assertTrue(profiles_keyed > 0L);
        }
    }
}
