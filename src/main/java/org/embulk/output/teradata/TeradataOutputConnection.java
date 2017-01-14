package org.embulk.output.teradata;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.JdbcOutputConnection;

public class TeradataOutputConnection
        extends JdbcOutputConnection
{
    private final Logger logger = Exec.getLogger(TeradataOutputConnection.class);

    public TeradataOutputConnection(Connection connection, String schemaName)
            throws SQLException
    {
        super(connection, schemaName);
    }
}
