package org.embulk.input.teradata;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.input.jdbc.JdbcOutputConnection;

public class TeradataOutputConnection
        extends JdbcOutputConnection
{
    private final Logger logger = Exec.getLogger(TeradataOutputConnection.class);

    public TeradataOutputConnection(Connection connection)
            throws SQLException
    {
        super(connection, null);
    }
}
