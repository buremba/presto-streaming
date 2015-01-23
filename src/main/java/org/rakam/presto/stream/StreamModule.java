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
package org.rakam.presto.stream;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import org.rakam.presto.stream.analyze.QueryAnalyzer;
import org.rakam.presto.stream.metadata.ForMetadata;
import org.rakam.presto.stream.metadata.StreamMetadata;
import org.rakam.presto.stream.metadata.TableColumn;
import org.rakam.presto.stream.query.StreamRecordSetProvider;
import org.rakam.presto.stream.storage.StreamStorageManager;
import org.rakam.presto.stream.storage.StreamPageSinkProvider;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StreamModule
        implements Module
{
    private final String connectorId;

    public StreamModule(String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connector id is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(StreamConnector.class).in(Scopes.SINGLETON);
        binder.bind(StreamConnectorId.class).toInstance(new StreamConnectorId(connectorId));
        binder.bind(StreamMetadata.class).in(Scopes.SINGLETON);
        binder.bind(StreamStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(StreamSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(StreamRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(StreamHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(StreamPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(QueryAnalyzer.class).in(Scopes.SINGLETON);
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = checkNotNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }

    @ForMetadata
    @Singleton
    @Provides
    public IDBI createDBI(@ForMetadata ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        DBI dbi = new DBI(connectionFactory);
        dbi.registerMapper(new TableColumn.Mapper(typeManager));
        return dbi;
    }
}
