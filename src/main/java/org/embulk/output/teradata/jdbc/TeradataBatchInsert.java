package org.embulk.output.teradata.jdbc;

import java.io.IOException;
import java.sql.Types;
import java.sql.SQLException;
import com.google.common.base.Optional;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.StandardBatchInsert;

public class TeradataBatchInsert
        extends StandardBatchInsert
{
    public TeradataBatchInsert(TeradataOutputConnector connector, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        super(connector, mergeConfig);
    }

    @Override
    public void setFloat(float v) throws IOException, SQLException
    {
        if (Float.isNaN(v) || Float.isInfinite(v)) {
            setNull(Types.REAL);  // TODO get through argument
        } else {
            super.setFloat(v);
        }
    }

    @Override
    public void setDouble(double v) throws IOException, SQLException
    {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            setNull(Types.DOUBLE);  // TODO get through argument
        } else {
            super.setDouble(v);
        }
    }
}