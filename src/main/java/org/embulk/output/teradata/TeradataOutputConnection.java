package org.embulk.output.teradata;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.input.jdbc.JdbcInputConnection;

public class TeradataOutputConnection
        extends JdbcInputConnection
{
    private final Logger logger = Exec.getLogger(TeradataOutputConnection.class);

    public TeradataOutputConnection(Connection connection)
            throws SQLException
    {
        super(connection, null);
    }
}
