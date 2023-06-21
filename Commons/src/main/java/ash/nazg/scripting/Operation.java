/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.scripting;

import ash.nazg.config.InvalidConfigurationException;
import ash.nazg.data.RDDUtils;
import ash.nazg.metadata.OperationMeta;
import ash.nazg.data.DataStream;
import org.apache.commons.collections4.map.ListOrderedMap;

import java.io.Serializable;
import java.util.Map;

public abstract class Operation implements Serializable {
    public final OperationMeta meta;

    protected RDDUtils rddUtils;

    protected ListOrderedMap<String, DataStream> inputStreams;
    protected ParamsContext params;
    protected ListOrderedMap<String, String> outputStreams;

    public Operation() {
        this.meta = meta();
    }

    public void initialize(RDDUtils rddUtils, ListOrderedMap<String, DataStream> input, ParamsContext params, ListOrderedMap<String, String> output) throws InvalidConfigurationException {
        this.rddUtils = rddUtils;

        this.inputStreams = input;
        this.params = params;
        this.outputStreams = output;

        configure();
    }

    abstract public OperationMeta meta();

    protected void configure() throws InvalidConfigurationException {
    }

    abstract public Map<String, DataStream> execute() throws InvalidConfigurationException;
}
