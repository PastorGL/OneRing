package ash.nazg.dist;

import java.io.IOException;
import java.io.InputStream;

public abstract class RecordInputStream extends InputStream {
    protected final int[] order;
    protected final char delimiter;

    protected byte[] recordBuffer;
    protected int position = 0;
    protected int size = 0;

    public RecordInputStream(int[] order, char delimiter) {
        this.order = order;
        this.delimiter = delimiter;
    }

    @Override
    public int read() throws IOException {
        ensureRecord();
        if (recordBuffer == null) {
            return -1;
        }

        int c = recordBuffer[position];
        position++;

        return c;
    }

    @Override
    public int read(byte[] b) throws IOException {
        ensureRecord();
        if (recordBuffer == null) {
            return -1;
        }

        int remaining = size - position;
        int len = Math.min(remaining, b.length);

        System.arraycopy(recordBuffer, position, b, 0, len);
        position += len;

        return len;
    }

    protected abstract void ensureRecord() throws IOException;
}
