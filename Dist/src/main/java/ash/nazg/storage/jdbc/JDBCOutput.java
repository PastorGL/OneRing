/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.jdbc;

import ash.nazg.config.tdl.Description;
import ash.nazg.storage.OutputAdapter;
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
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class JDBCOutput extends OutputAdapter {
    private static final Pattern PATTERN = Pattern.compile("^jdbc:(.+)");

    private String dbDriver;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;

    private int batchSize;

    private char delimiter;
    private String[] cols;

    @Override
    @Description("JDBC Output which performs batch INSERT VALUES of columns (in order of incidence)" +
            " into a table in the configured database")
    public Pattern proto() {
        return PATTERN;
    }

    @Override
    protected void configure() {
        dbDriver = outputResolver.get("jdbc.driver." + name);
        dbUrl = outputResolver.get("jdbc.url." + name);
        dbUser = outputResolver.get("jdbc.user." + name);
        dbPassword = outputResolver.get("jdbc.password." + name);

        batchSize = Integer.parseInt(outputResolver.get("jdbc.batch.size." + name, "500"));
        cols = dsResolver.outputColumns(name);
        delimiter = dsResolver.outputDelimiter(name);
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
