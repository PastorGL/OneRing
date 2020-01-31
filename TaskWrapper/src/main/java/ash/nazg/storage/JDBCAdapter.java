/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage;

public abstract class JDBCAdapter implements StorageAdapter {
    protected String dbDriver;
    protected String dbUrl;
    protected String dbUser;
    protected String dbPassword;

    protected char delimiter;
}
