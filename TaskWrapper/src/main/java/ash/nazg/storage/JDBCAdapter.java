package ash.nazg.storage;

public abstract class JDBCAdapter implements StorageAdapter {
    protected String dbDriver;
    protected String dbUrl;
    protected String dbUser;
    protected String dbPassword;

    protected char delimiter;
}
