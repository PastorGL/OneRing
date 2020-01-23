package ash.nazg.rest.endpoints;

import ash.nazg.rest.service.OperationService;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class OperationServiceTest extends TestBase {
    private static OperationService ops = injector.getInstance(OperationService.class);

    @Test
    public void testAvailableOperations() throws Exception {
        List<TaskDescriptionLanguage.Operation> operations = ops.getAvailableOperations();

        assertTrue(operations.size() > 0);
    }
}
