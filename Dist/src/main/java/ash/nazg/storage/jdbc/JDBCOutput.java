/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.jdbc;

import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.storage.OutputAdapter;
import ash.nazg.storage.metadata.AdapterMeta;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.sparkproject.guava.collect.Iterators;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import static ash.nazg.storage.jdbc.JDBCStorage.*;

@SuppressWarnings("unused")
public class JDBCOutput extends OutputAdapter {
    private static final String JDBC_PATTERN = "^jdbc:(.+)";
    private static final String BATCH_SIZE = "batch.size";

    private String dbDriver;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    private int batchSize;

    private char delimiter;
    private String[] cols;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("JDBC", "JDBC adapter which performs batch INSERT VALUES of columns (in order of incidence)" +
                " into a table in the configured database",
                JDBC_PATTERN,

                new DefinitionMetaBuilder()
                        .def(JDBC_DRIVER, "JDBC driver, fully qualified class name")
                        .def(JDBC_URL, "JDBC connection string URL")
                        .def(JDBC_USER, "JDBC connection user", null, "By default, user isn't set")
                        .def(JDBC_PASSWORD, "JDBC connection password", null, "By default, use no password")
                        .def(BATCH_SIZE, "Batch size for SQL INSERTs", Integer.class,
                                "500", "By default, use 500 records")
                        .build()
        );
    }

    @Override
    protected void configure() {
        dbDriver = outputResolver.definition(JDBC_DRIVER);
        dbUrl = outputResolver.definition(JDBC_URL);
        dbUser = outputResolver.definition(JDBC_USER);
        dbPassword = outputResolver.definition(JDBC_PASSWORD);

        batchSize = outputResolver.definition(BATCH_SIZE);

        cols = dsResolver.outputColumns(dsName);
        delimiter = dsResolver.outputDelimiter(dsName);
    }

    @Override
    public void save(String path, JavaRDD<Text> rdd) {
        final String _dbDriver = dbDriver;
        final String _dbUrl = dbUrl;
        final String _dbUser = dbUser;
        final String _dbPassword = dbPassword;

        int _batchSize = batchSize;

        final char _delimiter = delimiter;
        final String[] _cols = cols;
        final String _table = path.split(":", 2)[1];

        rdd.mapPartitions(partition -> {
            Connection conn = null;
            PreparedStatement ps = null;
            try {
                Class.forName(_dbDriver);

                Properties properties = new Properties();
                properties.setProperty("user", _dbUser);
                properties.setProperty("password", _dbPassword);

                conn = DriverManager.getConnection(_dbUrl, properties);

                CSVParser parser = new CSVParserBuilder().withSeparator(_delimiter).build();

                StringBuilder sb = new StringBuilder("INSERT INTO " + _table + " VALUES ");
                sb.append("(");
                for (int i = 0, j = 0; i < _cols.length; i++) {
                    if (!_cols[i].equals("_")) {
                        if (j > 0) {
                            sb.append(",");
                        }
                        sb.append("?");
                        j++;
                    }
                }
                sb.append(")");

                ps = conn.prepareStatement(sb.toString());
                int b = 0;
                while (partition.hasNext()) {
                    String v = String.valueOf(partition.next());

                    String[] row = parser.parseLine(v);
                    for (int i = 0, j = 1; i < _cols.length; i++) {
                        if (!_cols[i].equals("_")) {
                            ps.setObject(j++, row[i]);
                        }
                    }
                    ps.addBatch();

                    if (b == _batchSize) {
                        ps.executeBatch();

                        ps.clearBatch();
                        b = 0;
                    }

                    b++;
                }
                if (b != 0) {
                    ps.executeBatch();
                }

                return Iterators.emptyIterator();
            } catch (SQLException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            } finally {
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }
        }).count();
    }
}
