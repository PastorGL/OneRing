/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import ash.nazg.rest.service.TaskService;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TaskServiceTest extends TestBase {
    private static TaskService tvs = injector.getInstance(TaskService.class);

    @Ignore
    @Test
    public void testValidateTask() throws Exception {
/*        TestRunner underTestLoad = new TestRunner();
        underTestLoad.before("/config.task.properties");

        Properties loaded = underTestLoad.getTaskConfig().getProperties();
        TaskDefinitionLanguage.Task task = PropertiesConverter.toTask(null, loaded);

        Properties props = tvs.validateTask(task);

        assertEquals(loaded.size(), props.size());*/
    }
}
