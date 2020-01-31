/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.output;

import ash.nazg.storage.HDFSAdapter;

import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class HDFSOutput extends HadoopOutput {
    @Override
    public Pattern proto() {
        return HDFSAdapter.PATTERN;
    }
}
