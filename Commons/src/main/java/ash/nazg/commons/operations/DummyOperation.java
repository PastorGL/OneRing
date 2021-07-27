/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class DummyOperation extends Operation {
    private String[] inputNames;

    private String[] outputNames;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("nop", "This operation does nothing, just passes all its inputs as all outputs",

                new PositionalStreamsMetaBuilder()
                        .ds("Any number of inputs to be renamed",
                                new StreamType[]{StreamType.Plain, StreamType.KeyValue, StreamType.CSV}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .ds("Newly named inputs (must be same count as inputs)",
                                new StreamType[]{StreamType.Passthru}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputNames = opResolver.positionalInputs();
        outputNames = opResolver.positionalOutputs();

        if (inputNames.length != outputNames.length) {
            throw new InvalidConfigValueException("Operation '" + name + "' must have equal number of inputs and outputs");
        }
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
