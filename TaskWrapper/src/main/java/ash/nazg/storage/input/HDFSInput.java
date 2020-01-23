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
