package org.embulk.output.teradata;

import java.util.List;
import java.util.Properties;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;
import java.sql.Connection;
import com.google.common.base.Optional;
import org.embulk.config.TaskReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.output.teradata.TeradataOutputConnection;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class TeradataOutputPlugin
        implements OutputPlugin
{

    private final Logger logger = Exec.getLogger(TeradataOutputPlugin.class);
    private static final Driver driver = new com.teradata.jdbc.TeraDriver();

    public interface TeradataPluginTask
            extends org.embulk.input.jdbc.AbstractJdbcInputPlugin.PluginTask
    {
      @Config("host")
      public String getHost();

      @Config("port")
      @ConfigDefault("1025")
      public int getPort();

      @Config("user")
      public String getUser();

      @Config("password")
      @ConfigDefault("\"\"")
      public String getPassword();

      @Config("database")
      public String getDatabase();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        org.embulk.input.jdbc.AbstractJdbcInputPlugin.PluginTask task = config.loadConfig(org.embulk.input.jdbc.AbstractJdbcInputPlugin.PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("teradata output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        TeradataPluginTask t = taskSource.loadTask(TeradataPluginTask.class);

        String url = String.format("jdbc:teradata://%s",t.getHost());

        Properties props = new Properties();
	props.setProperty("database", t.getDatabase());
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());

        props.putAll(t.getOptions());


	Connection con = null;
        try {
	    con = driver.connect(url, props);
            Statement stmt = con.createStatement();
            String sql = String.format("select * from dbc.tables;");
            logger.info("SQL: " + sql);
            stmt.execute(sql);

            TeradataOutputConnection c = new TeradataOutputConnection(con);
            con = null;
	    return null;
        }
	catch(SQLException se){
	}
        finally {
	    /*
            if (con != null) {
                con.close();
		}*/
        }
	return null;
    }
}
