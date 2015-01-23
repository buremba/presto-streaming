package org.rakam.presto.stream.metadata;

import com.facebook.presto.spi.SchemaTableName;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;

public interface MetadataDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS tables (\n" +
            "  table_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  catalog_name VARCHAR(255) NOT NULL,\n" +
            "  schema_name VARCHAR(255) NOT NULL,\n" +
            "  table_name VARCHAR(255) NOT NULL,\n" +
            "  is_grouped bool NOT NULL,\n" +
            "  UNIQUE (catalog_name, schema_name, table_name)\n" +
            ")")
    void createTableTables();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS columns (\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  column_name VARCHAR(255) NOT NULL,\n" +
            "  ordinal_position INT NOT NULL,\n" +
            "  data_type VARCHAR(255) NOT NULL,\n" +
            "  is_aggregation_field bool NOT NULL,\n" +
            "  signature binary DEFAULT NULL,\n" +
            "  sort_ordinal_position INT DEFAULT NULL,\n" +
            "  PRIMARY KEY (table_id, column_name),\n" +
            "  UNIQUE (table_id, column_name),\n" +
            "  UNIQUE (table_id, ordinal_position),\n" +
            "  FOREIGN KEY (table_id) REFERENCES tables (table_id)\n" +
            ")")
    void createTableColumns();

    @SqlQuery("SELECT table_id, is_grouped FROM tables\n" +
            "WHERE catalog_name = :catalogName\n" +
            "  AND schema_name = :schemaName\n" +
            "  AND table_name = :tableName")
    @Mapper(Table.TableMapper.class)
    Table getTableInformation(
            @Bind("catalogName") String catalogName,
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT t.schema_name, t.table_name, c.column_name, c.ordinal_position, c.signature, c.is_aggregation_field, c.data_type\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n" +
            "WHERE t.table_id = :tableId\n" +
            "  AND c.column_name = :columnName\n" +
            "ORDER BY c.ordinal_position\n")
    TableColumn getTableColumn(
            @Bind("tableId") long tableId,
            @Bind("columnName") String columnName);

    @SqlQuery("SELECT t.schema_name, t.table_name, c.column_name, c.is_aggregation_field, c.signature, c.ordinal_position, c.data_type\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n" +
            "WHERE t.table_id = :tableId\n" +
            "ORDER BY c.ordinal_position")
    List<TableColumn> getTableColumns(@Bind("tableId") long tableId);

    @SqlQuery("SELECT schema_name, table_name\n" +
            "FROM tables\n" +
            "WHERE catalog_name = :catalogName\n" +
            "  AND (schema_name = :schemaName OR :schemaName IS NULL)")
    @Mapper(SchemaTableNameMapper.class)
    List<SchemaTableName> listTables(
            @Bind("catalogName") String catalogName,
            @Bind("schemaName") String schemaName);

    @SqlQuery("SELECT DISTINCT schema_name FROM tables\n" +
            "WHERE catalog_name = :catalogName\n")
    List<String> listSchemaNames(@Bind("catalogName") String catalogName);

    @SqlQuery("SELECT t.schema_name, t.table_name, c.column_name, c.is_aggregation_field, c.signature, c.ordinal_position, c.data_type\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n" +
            "WHERE catalog_name = :catalogName\n" +
            "  AND (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY schema_name, table_name, ordinal_position")
    List<TableColumn> listTableColumns(
            @Bind("catalogName") String catalogName,
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT t.schema_name, t.table_name, c.column_name, c.is_aggregation_field, c.signature, c.ordinal_position, c.data_type\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n" +
            "WHERE t.table_id = :tableId\n" +
            "ORDER BY c.ordinal_position")
    List<TableColumn> listTableColumns(@Bind("tableId") long tableId);

    @SqlQuery("SELECT t.schema_name, t.table_name, c.column_name, c.is_aggregation_field, c.signature, c.ordinal_position, c.data_type\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n" +
            "WHERE t.table_id = :tableId\n" +
            "  AND c.sort_ordinal_position IS NOT NULL\n" +
            "ORDER BY c.sort_ordinal_position")
    List<TableColumn> listSortColumns(@Bind("tableId") long tableId);

    @SqlUpdate("INSERT INTO tables (catalog_name, schema_name, table_name, is_grouped)\n" +
            "VALUES (:catalogName, :schemaName, :tableName, :isGrouped)")
    @GetGeneratedKeys
    long insertTable(
            @Bind("catalogName") String catalogName,
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName,
            @Bind("isGrouped") boolean isGrouped);

    @SqlUpdate("UPDATE tables SET\n" +
            "  schema_name = :newSchemaName\n" +
            ", table_name = :newTableName\n" +
            "WHERE table_id = :tableId")
    void renameTable(
            @Bind("tableId") long tableId,
            @Bind("newSchemaName") String newSchemaName,
            @Bind("newTableName") String newTableName);


    @SqlUpdate("DELETE FROM columns WHERE table_id = :tableId")
    int dropColumns(@Bind("tableId") long tableId);

    @SqlUpdate("DELETE FROM tables WHERE table_id = :tableId")
    int dropTable(@Bind("tableId") long tableId);

    @SqlUpdate("INSERT INTO columns (table_id, column_name, ordinal_position, is_aggregation_field, signature, data_type)\n" +
            "VALUES (:tableId, :columnName, :ordinalPosition, :isAggregationField, :signature, :dataType)")
    void insertColumn(
            @Bind("tableId") long tableId,
            @Bind("columnName") String columnName,
            @Bind("signature") byte[] signature,
            @Bind("ordinalPosition") int ordinalPosition,
            @Bind("isAggregationField") boolean isAggregationField,
            @Bind("dataType") String dataType);
}
