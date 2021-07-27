package ash.nazg.config.tdl.metadata;

import java.util.Map;

public class OperationMeta {
    public final String verb;
    public final String descr;

    public final DataStreamsMeta input;
    public final DataStreamsMeta output;

    public final Map<String, DefinitionMeta> definitions;

    public OperationMeta(String verb, String descr, DataStreamsMeta input, Map<String, DefinitionMeta> definitions, DataStreamsMeta output) {
        this.verb = verb;
        this.descr = descr;

        this.input = input;
        this.output = output;

        this.definitions = definitions;
    }
}
