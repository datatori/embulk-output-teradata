package org.embulk.output.teradata.jdbc;

import org.embulk.output.jdbc.JdbcOutputConnector;

import java.util.Properties;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;

public class TeradataOutputConnector
        implements JdbcOutputConnector
{
    private final Driver driver;
    private final String url;
    private final Properties properties;

    public TeradataOutputConnector(String url, Properties properties)
    {
        try
        {
            this.driver = new com.teradata.jdbc.TeraDriver();
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
        this.url = url;
        this.properties = properties;
    }

    @Override
    public TeradataOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = driver.connect(url, properties);

        try {
            TeradataOutputConnection con = new TeradataOutputConnection(c, autoCommit);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}