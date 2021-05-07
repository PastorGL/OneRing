/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.storage.jdbc;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.storage.InputAdapter;
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
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class JDBCInput extends InputAdapter {
    private static final Pattern PATTERN = Pattern.compile("^jdbc:SELECT.+?\\?.+?\\?.*");

    private JavaSparkContext ctx;
    private int partCount;
    private String dbDriver;
    private String dbUrl;
    private String dbUser;
    private String dbPassword;
    private char delimiter;

    @Override
    @Description("JDBC Input from an SQL SELECT query against a configured database." +
            " Must use numeric boundaries for each part")
    public Pattern proto() {
        return PATTERN;
    }

    @Override
    protected void configure() throws InvalidConfigValueException {
        dbDriver = inputResolver.get("jdbc.driver." + name);
        dbUrl = inputResolver.get("jdbc.url." + name);
        dbUser = inputResolver.get("jdbc.user." + name);
        dbPassword = inputResolver.get("jdbc.password." + name);

        partCount = dsResolver.inputParts(name);
        delimiter = dsResolver.inputDelimiter(name);
    }

    @Override
    public JavaRDD<Text> load(String path) {
        final char _inputDelimiter = delimiter;

        return new JdbcRDD<Object[]>(
                ctx.sc(),
                new DbConnection(dbDriver, dbUrl, dbUser, dbPassword),
                path.split(":", 2)[1],
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
            properties.setProperty("user", _dbUser);
            properties.setProperty("password", _dbPassword);

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
