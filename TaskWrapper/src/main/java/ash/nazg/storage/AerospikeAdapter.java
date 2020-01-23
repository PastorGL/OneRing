package ash.nazg.storage;

public abstract class AerospikeAdapter implements StorageAdapter {
    protected Integer aerospikePort;
    protected String aerospikeHost;

    protected char delimiter;
}
