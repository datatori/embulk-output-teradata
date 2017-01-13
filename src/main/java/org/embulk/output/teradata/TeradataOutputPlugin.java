package org.embulk.output.teradata;

import java.util.List;
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
import org.embulk.input.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.input.teradata.TeradataOutputConnection;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class TeradataOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
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
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        return resume(task.dump(), schema, taskCount, control);

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
        PluginTask t = taskSource.loadTask(PluginTask.class);

        String url = String.format("jdbc:teradata://%s",t.getHost());

        Properties props = new Properties();
	      props.setProperty("database", t.getDatabase());
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());

        props.putAll(t.getOptions());

        Connection con = driver.connect(url, props);

        try {
            Statement stmt = con.createStatement();
            String sql = String.format("select * from dbc.tables;");
            logger.info("SQL: " + sql);
            stmt.execute(sql);

            TeradataInputConnection c = new TeradataInputConnection(con);
            con = null;
            return c;
        }
        finally {
            if (con != null) {
                con.close();
            }
        }
    }
}
