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
