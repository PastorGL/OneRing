/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.jdbc;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.storage.InputAdapter;
import ash.nazg.storage.metadata.AdapterMeta;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static ash.nazg.storage.jdbc.JDBCStorage.*;

@SuppressWarnings("unused")
public class JDBCInput extends InputAdapter {
    private static final String SELECT_PATTERN = "^jdbc:SELECT.+?\\?.+?\\?.*";

    private JavaSparkContext ctx;
    private int partCount;
    private String dbDriver;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private char delimiter;

    @Override
    protected AdapterMeta meta() {
        return new AdapterMeta("JDBC", "JDBC adapter for reading data from an SQL SELECT query against" +
                " a configured database. Must use numeric boundaries for each part denoted by two ? placeholders," +
                " for example, SELECT * FROM table WHERE integer_key BETWEEN ? AND ?",
                SELECT_PATTERN,

                new DefinitionMetaBuilder()
                        .def(JDBC_DRIVER, "JDBC driver, fully qualified class name")
                        .def(JDBC_URL, "JDBC connection string URL")
                        .def(JDBC_USER, "JDBC connection user", null, "By default, user isn't set")
                        .def(JDBC_PASSWORD, "JDBC connection password", null, "By default, use no password")
                        .build()
        );
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        dbDriver = inputResolver.definition(JDBC_DRIVER);
        dbUrl = inputResolver.definition(JDBC_URL);
        dbUser = inputResolver.definition(JDBC_USER);
        dbPassword = inputResolver.definition(JDBC_PASSWORD);

        partCount = dsResolver.inputParts(dsName);
        delimiter = dsResolver.inputDelimiter(dsName);
    }

    @Override
    public JavaRDD<Text> load(String query) {
        final char _inputDelimiter = delimiter;

        return new JdbcRDD<Object[]>(
                ctx.sc(),
                new DbConnection(dbDriver, dbUrl, dbUser, dbPassword),
                query.split(":", 2)[1],
                0, Math.max(partCount, 0),
                Math.max(partCount, 1),
                new RowMapper(),
                ClassManifestFactory$.MODULE$.fromClass(Object[].class)
        ).toJavaRDD()
                .mapPartitions(it -> {
                    List<Text> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        Object[] v = it.next();

                        String[] acc = new String[v.length];

                        int i = 0;
                        for (Object col : v) {
                            acc[i++] = String.valueOf(col);
                        }

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _inputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(acc, false);
                        writer.close();

                        ret.add(new Text(buffer.toString()));
                    }

                    return ret.iterator();
                });
    }

    static class DbConnection extends AbstractFunction0<Connection> implements Serializable {
        final String _dbDriver;
        final String _dbUrl;
        final String _dbUser;
        final String _dbPassword;

        DbConnection(String _dbDriver, String _dbUrl, String _dbUser, String _dbPassword) {
            this._dbDriver = _dbDriver;
            this._dbUrl = _dbUrl;
            this._dbUser = _dbUser;
            this._dbPassword = _dbPassword;
        }

        @Override
        public Connection apply() {
            try {
                Class.forName(_dbDriver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            Properties properties = new Properties();
            if (_dbUser != null) {
                properties.setProperty("user", _dbUser);
            }
            if (_dbPassword != null) {
                properties.setProperty("password", _dbPassword);
            }

            Connection connection = null;
            try {
                connection = DriverManager.getConnection(_dbUrl, properties);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return connection;
        }
    }

    static class RowMapper extends AbstractFunction1<ResultSet, Object[]> implements Serializable {
        @Override
        public Object[] apply(ResultSet row) {
            return JdbcRDD.resultSetToObjectArray(row);
        }
    }
}
