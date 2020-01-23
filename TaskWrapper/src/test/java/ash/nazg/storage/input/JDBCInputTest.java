package ash.nazg.storage.input;

import ash.nazg.spark.TestTaskWrapper;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class JDBCInputTest {
    @Ignore
    @Test
    public void testInput() throws Exception {
        try (TestTaskWrapper underTest = new TestTaskWrapper(false, "/config.JDBCInput.properties")) {
            underTest.go();
            Map<String, JavaRDDLike> res = underTest.getResult();

            long profiles_keyed = res.get("p2").count();
            assertTrue(profiles_keyed > 0L);

        }
    }
}
