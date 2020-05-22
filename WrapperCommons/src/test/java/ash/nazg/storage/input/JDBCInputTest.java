/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.input;

import ash.nazg.storage.TestStorageWrapper;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class JDBCInputTest {
    @Ignore
    @Test
    public void testInput() throws Exception {
        try (TestStorageWrapper underTest = new TestStorageWrapper(false, "/config.JDBCInput.properties")) {
            underTest.go();
            Map<String, JavaRDDLike> res = underTest.result;

            long profiles_keyed = res.get("p2").count();
            assertTrue(profiles_keyed > 0L);
        }
    }
}
