package org.rakam.presto.stream.metadata;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TableColumn
{
    private final SchemaTableName table;
    private final String columnName;
    private final int ordinalPosition;
    private final Type dataType;
    private final @Nullable Signature signature;
    private boolean isAggregationField;

    public TableColumn(SchemaTableName table, String columnName, Signature signature, boolean isAggregationField, int ordinalPosition, Type dataType)
    {
        this.table = checkNotNull(table, "table is null");
        this.columnName = checkNotNull(columnName, "columnName is null");
        checkArgument(ordinalPosition >= 0, "ordinal position is negative");
        this.ordinalPosition = ordinalPosition;
        this.signature = signature;
        this.isAggregationField = isAggregationField;
        this.dataType = checkNotNull(dataType, "dataType is null");
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public Signature getSignature()
    {
        return signature;
    }

    public Type getDataType()
    {
        return dataType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(table, columnName, ordinalPosition, dataType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TableColumn o = (TableColumn) obj;
        return Objects.equal(table, o.table) &&
                Objects.equal(columnName, o.columnName) &&
                Objects.equal(ordinalPosition, o.ordinalPosition) &&
                Objects.equal(dataType, o.dataType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("columnName", columnName)
                .add("ordinalPosition", ordinalPosition)
                .add("dataType", dataType)
                .toString();
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, dataType, ordinalPosition, false);
    }

    public boolean getIsAggregationField() {
        return isAggregationField;
    }

    public static class Mapper
            implements ResultSetMapper<TableColumn>
    {
        private final TypeManager typeManager;
        private final static ObjectMapper mapper = new ObjectMapper();

        @Inject
        public Mapper(TypeManager typeManager)
        {
            this.typeManager = checkNotNull(typeManager, "typeManager is null");
        }

        @Override
        public TableColumn map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            SchemaTableName table = new SchemaTableName(
                    r.getString("schema_name"),
                    r.getString("table_name"));

            String typeName = r.getString("data_type");
            Type type = typeManager.getType(parseTypeSignature(typeName));
            checkArgument(type != null, "Unknown type %s", typeName);


            byte[] serializedSignature = r.getBytes("signature");

            Signature signature = null;
            if(serializedSignature!=null) {
                try {
                    signature = mapper.readValue(serializedSignature, Signature.class);
                } catch (IOException e) {
                    //
                }
            }

            return new TableColumn(
                    table,
                    r.getString("column_name"),
                    signature,
                    r.getBoolean("is_aggregation_field"),
                    r.getInt("ordinal_position"),
                    type);
        }
    }
}