/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.input;

import ash.nazg.storage.HDFSAdapter;

import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class HDFSInput extends HadoopInput {
    @Override
    public Pattern proto() {
        return HDFSAdapter.PATTERN;
    }
}
