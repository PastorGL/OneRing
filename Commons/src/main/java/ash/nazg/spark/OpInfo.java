package ash.nazg.spark;

import ash.nazg.config.tdl.TaskDescriptionLanguage;

public class OpInfo {
    public final String verb;
    public final Class<? extends Operation> opClass;
    public final TaskDescriptionLanguage.Operation description;

    public OpInfo(String verb, Class<? extends Operation> opClass, TaskDescriptionLanguage.Operation description) {
        this.verb = verb;
        this.opClass = opClass;
        this.description = description;
    }
}
