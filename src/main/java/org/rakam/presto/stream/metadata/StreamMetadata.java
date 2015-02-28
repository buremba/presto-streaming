/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.presto.stream.metadata;

import com.facebook.presto.Session;
import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.rakam.presto.stream.StreamColumnHandle;
import org.rakam.presto.stream.StreamConnectorId;
import org.rakam.presto.stream.StreamErrorCode;
import org.rakam.presto.stream.StreamTableHandle;
import org.rakam.presto.stream.analyze.AggregationQuery;
import org.rakam.presto.stream.analyze.QueryAnalyzer;
import org.rakam.presto.stream.storage.MaterializedView;
import org.rakam.presto.stream.storage.GroupByRowTable;
import org.rakam.presto.stream.storage.SimpleRowTable;
import org.rakam.presto.stream.storage.StreamInsertTableHandle;
import org.rakam.presto.stream.storage.StreamStorageManager;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static org.rakam.presto.stream.util.Types.checkType;

public class StreamMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(StreamMetadata.class);

    private final String connectorId;
    private final IDBI dbi;

    private final StreamStorageManager storageAdapter;
    private final QueryAnalyzer queryAnalyzer;
    private final MetadataDao dao;

    @Inject
    public StreamMetadata(StreamConnectorId connectorId, @ForMetadata IDBI dbi, StreamStorageManager storageAdapter, QueryAnalyzer queryAnalyzer) throws InterruptedException {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.queryAnalyzer = checkNotNull(queryAnalyzer, "localQueryRunner is null");
        this.dbi = dbi;
        this.dao = dbi.onDemand(MetadataDao.class);
        this.storageAdapter = storageAdapter;

        Duration delay = new Duration(10, TimeUnit.SECONDS);
        while (true) {
            try {
                dao.createTableTables();
                dao.createTableColumns();
                return;
            }
            catch (UnableToObtainConnectionException e) {
                log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                sleep(delay.toMillis());
            }
        }
//        db = DBMaker.newFileDB(new File("./test")).make().createHashMap("metadata").make();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return dao.listSchemaNames(connectorId);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(tableName);
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        Table table = dao.getTableInformation(connectorId, tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        List<TableColumn> tableColumns = dao.getTableColumns(table.getTableId());
        checkArgument(!tableColumns.isEmpty(), "Table %s does not have any columns", tableName);

        StreamColumnHandle countColumnHandle = null;
        for (TableColumn tableColumn : tableColumns) {
            if (countColumnHandle == null && tableColumn.getDataType().getJavaType().isPrimitive()) {
                countColumnHandle = getColumnHandle(tableColumn);
            }
        }

        return new StreamTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getTableId());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        StreamTableHandle handle = checkType(tableHandle, StreamTableHandle.class, "tableHandle");
        SchemaTableName tableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
        List<ColumnMetadata> columns = dao.getTableColumns(handle.getTableId()).stream()
                .map(TableColumn::toColumnMetadata)
                .collect(toList());
        if (columns.isEmpty()) {
            throw new PrestoException(StreamErrorCode.STREAM_ERROR, "Table does not have any columns: " + tableName);
        }
        return new ConnectorTableMetadata(tableName, columns);
    }

    public Table getTable(String schemaName, String tableName) {
        return dao.getTableInformation(connectorId, schemaName, tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return dao.listTables(connectorId, schemaNameOrNull);
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return false;
    }

    private StreamColumnHandle getColumnHandle(TableColumn tableColumn)
    {
        return new StreamColumnHandle(connectorId, tableColumn.getColumnName(), tableColumn.getOrdinalPosition(), tableColumn.getDataType(),  tableColumn.getIsAggregationField());
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        StreamTableHandle raptorTableHandle = checkType(tableHandle, StreamTableHandle.class, "tableHandle");
        ImmutableMap.Builder<String, ConnectorColumnHandle> builder = ImmutableMap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(raptorTableHandle.getTableId())) {
            builder.put(tableColumn.getColumnName(), getColumnHandle(tableColumn));
        }
        return builder.build();
    }


    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableListMultimap.Builder<SchemaTableName, ColumnMetadata> columns = ImmutableListMultimap.builder();
        for (TableColumn tableColumn : dao.listTableColumns(connectorId, prefix.getSchemaName(), prefix.getTableName())) {
            ColumnMetadata columnMetadata = new ColumnMetadata(tableColumn.getColumnName(), tableColumn.getDataType(), tableColumn.getOrdinalPosition(), false);
            columns.put(tableColumn.getTable(), columnMetadata);
        }
        return Multimaps.asMap(columns.build());
    }

    @Override
    public ConnectorTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Create table is not supported. Use create view for stream data.");
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Create table is not supported. Use create view for stream data.");
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Create table is not supported. Use create view for stream data.");
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Create table is not supported. Use create view for stream data.");
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<String> fragments)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Create table is not supported. Use create view for stream data.");
    }

    private List<StreamColumnHandle> getSortColumnHandles(long tableId)
    {
        ImmutableList.Builder<StreamColumnHandle> builder = ImmutableList.builder();
        for (TableColumn tableColumn : dao.listSortColumns(tableId)) {
            builder.add(getColumnHandle(tableColumn));
        }
        return builder.build();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StreamTableHandle tableHandle1 = checkType(tableHandle, StreamTableHandle.class, "tableHandle");
        long tableId = tableHandle1.getTableId();

        ImmutableList.Builder<StreamColumnHandle> columnHandles = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (TableColumn column : dao.getTableColumns(tableId)) {
            columnHandles.add(new StreamColumnHandle(connectorId, column.getColumnName(), column.getOrdinalPosition(), column.getDataType(), column.getIsAggregationField()));
            columnTypes.add(column.getDataType());
        }

        String externalBatchId = session.getProperties().get("external_batch_id");
        List<StreamColumnHandle> sortColumnHandles = getSortColumnHandles(tableId);
        return new StreamInsertTableHandle(connectorId,
                tableId,
                columnHandles.build(),
                columnTypes.build(),
                externalBatchId,
                sortColumnHandles,
                nCopies(sortColumnHandles.size(), ASC_NULLS_FIRST));
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<String> fragments)
    {
    }

    @Override
    public void createView(ConnectorSession userSession, SchemaTableName viewName, String viewData, boolean replace)
    {
        JsonNode tree;

        try {
            tree = new ObjectMapper().readTree(viewData);
        }
        catch (Exception e) {
            return;
        }

        String originalSql = tree.get("originalSql").asText();

        Session session = Session.builder()
            .setUser("user")
            .setSource("test")
            .setCatalog("stream")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .build();

        AggregationQuery execute = queryAnalyzer.execute(session, originalSql);

        if (replace) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "View replace is not supported.");
        }

        Long newTableId = dbi.inTransaction((handle, status) -> {
            MetadataDao dao = handle.attach(MetadataDao.class);
            long tableId = 0;
            try {
                tableId = dao.insertTable(connectorId, viewName.getSchemaName(), viewName.getTableName(), execute.isGroupByQuery());
            } catch (UnableToExecuteStatementException e) {
                if (e.getCause() instanceof SQLException) {
                    String state = ((SQLException) e.getCause()).getSQLState();
                    if (state.startsWith("23")) {
                        return null;
                    }
                }
                throw e;
            }

            for (QueryAnalyzer.AggregationField field : execute.aggregationFields) {
                TypeSignature typeSignature = field.colType.getTypeSignature();
                byte[] bytes = new ObjectMapper().writeValueAsBytes(field.functionSignature);
                dao.insertColumn(tableId, field.colName, bytes, field.position, true, typeSignature.toString());
            }

            for (QueryAnalyzer.GroupByField field : execute.groupByFields) {
                TypeSignature typeSignature = field.colType.getTypeSignature();
                dao.insertColumn(tableId, field.colName, null, field.position, false, typeSignature.toString());
            }
            return tableId;
        });

        if (newTableId == null) {
            throw new PrestoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }

        MaterializedView view;
        if (!execute.isGroupByQuery()) {
            view = new SimpleRowTable(execute.aggregationFields.stream()
                    .map(x -> x.accumulatorFactory.createAccumulator())
                    .toArray(Accumulator[]::new));
        } else {
            GroupedAccumulator[] groupedAggregations = execute.aggregationFields.stream()
                    .map(x -> x.accumulatorFactory.createGroupedAccumulator())
                    .toArray(GroupedAccumulator[]::new);

            List<Type> types = execute.groupByFields.stream().map(x -> x.colType).collect(Collectors.toList());
            int[] positions = execute.groupByFields.stream().mapToInt(x -> x.position).toArray();

            GroupByHash groupByHash = new GroupByHash(types, positions, Optional.empty(), 10000);
            view = new GroupByRowTable(groupedAggregations, groupByHash, positions);
        }

        storageAdapter.addMaterializedView(newTableId, view);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
//        dao.dropTable(viewName.getSchemaName());
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return Lists.newArrayList();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return Maps.newHashMap();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        long tableId = checkType(tableHandle, StreamTableHandle.class, "tableHandle").getTableId();
        String columnName = checkType(columnHandle, StreamColumnHandle.class, "columnHandle").getColumnName();

        TableColumn tableColumn = dao.getTableColumn(tableId, columnName);
        if (tableColumn == null) {
            throw new PrestoException(NOT_FOUND, format("Column %s does not exist for table ID %s", columnName, tableId));
        }
        return tableColumn.toColumnMetadata();
    }

    public QueryAnalyzer getQueryAnalyzer() {
        return null;
    }
}
