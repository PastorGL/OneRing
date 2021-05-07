/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class DummyOperation extends Operation {

    public static final String VERB = "nop";

    private String[] inputNames;
    private String[] outputNames;

    @Override
    @Description("This operation does nothing, just passes all its inputs as all outputs")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.Plain, StreamType.KeyValue, StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.Passthru},
                                false
                        )
                )
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputNames = opResolver.positionalInputs();
        outputNames = opResolver.positionalOutputs();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        Map<String, JavaRDDLike> outs = new HashMap<>();

        for (int i = 0; i < inputNames.length; i++) {
            outs.put(outputNames[i], input.get(inputNames[i]));
        }

        return outs;
    }
}
