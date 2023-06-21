/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.data;

import ash.nazg.scripting.ParamsContext;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface StreamConverter {
    DataStream apply(DataStream ds, Map<String, List<String>> newColumns, ParamsContext params);
}
