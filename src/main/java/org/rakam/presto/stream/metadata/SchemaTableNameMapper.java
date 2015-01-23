package org.rakam.presto.stream.metadata;

import com.facebook.presto.spi.SchemaTableName;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/01/15 10:26.
 */
public class SchemaTableNameMapper
        implements ResultSetMapper<SchemaTableName>
{
    @Override
    public SchemaTableName map(int index, ResultSet r, StatementContext ctx)
            throws SQLException
    {
        return new SchemaTableName(
                r.getString("schema_name"),
                r.getString("table_name"));
    }
}

