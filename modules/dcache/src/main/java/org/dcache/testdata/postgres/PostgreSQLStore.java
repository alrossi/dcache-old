package org.dcache.testdata.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.util.Properties;

import org.dcache.db.AlarmEnabledDataSource;
import org.dcache.testdata.Store;

public class PostgreSQLStore extends Store {
    private static final String driver = "org.postgresql.Driver";
    private static final String url = "jdbc:postgresql://localhost/test";
    private static final String user = "test";

    public PostgreSQLStore() throws IOException {
        super(url);
    }

    public PostgreSQLStore(HikariConfig config) throws IOException {
        super(url);
        config.setJdbcUrl(url);
        config.setUsername(user);
        pmf.setConnectionFactory(new HikariDataSource(config));

    }

    public PostgreSQLStore(HikariConfig config, String connectorName)
                    throws IOException {
        super(url);
        config.setJdbcUrl(url);
        config.setUsername(user);
        pmf.setConnectionFactory(new AlarmEnabledDataSource(url,
                                                            connectorName,
                                                            new HikariDataSource(config)));
    }

    @Override
    protected void setOptionalProperties(Properties properties) {
        properties.setProperty("javax.jdo.option.ConnectionDriverName", driver);
        properties.setProperty("javax.jdo.option.ConnectionUserName", user);
        properties.setProperty("javax.jdo.option.DetachAllOnCommit", "true");
        properties.setProperty("javax.jdo.option.Optimistic", "true");
        properties.setProperty("javax.jdo.option.NontransactionalRead", "true");
        properties.setProperty("javax.jdo.option.RetainValues", "true");
        properties.setProperty("javax.jdo.option.Multithreaded", "true");
        properties.setProperty("datanucleus.autoCreateSchema", "true");
        properties.setProperty("datanucleus.validateTables", "false");
        properties.setProperty("datanucleus.validateConstraints", "false");
        properties.setProperty("datanucleus.autoCreateColumns", "true");
        properties.setProperty("datanucleus.connectionPoolingType", "None");
    }
}
