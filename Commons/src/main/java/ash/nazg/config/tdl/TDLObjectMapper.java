/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class TDLObjectMapper extends ObjectMapper {
    public TDLObjectMapper() {
        super();

        enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        enable(SerializationFeature.INDENT_OUTPUT);
        enable(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED);
    }
}
