package org.embulk.output.teradata;

import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.teradata.jdbc.TeradataBatchInsert;
import org.embulk.output.teradata.jdbc.TeradataOutputConnector;

public class TeradataOutputPlugin
        extends AbstractJdbcOutputPlugin
{

    public interface TeradataPluginTask
            extends PluginTask
    {
        @Config("url")
        public String getUrl();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();

        @Config("max_table_name_length")
        @ConfigDefault("30")
        public int getMaxTableNameLength();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return TeradataPluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        TeradataPluginTask t = (TeradataPluginTask) task;
        return new Features()
            .setMaxTableNameLength(t.getMaxTableNameLength())
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE));
    }

    @Override
    protected TeradataOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        TeradataPluginTask t = (TeradataPluginTask) task;

        Properties props = new Properties();

        props.putAll(t.getOptions());

        if (t.getUser().isPresent()) {
            props.setProperty("user", t.getUser().get());
        }
        logger.info("Connecting to {} options {}", t.getUrl(), props);
        if (t.getPassword().isPresent()) {
            props.setProperty("password", t.getPassword().get());
        }

        return new TeradataOutputConnector(t.getUrl(), props);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        return new TeradataBatchInsert(getConnector(task, true), mergeConfig);
    }
}
