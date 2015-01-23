package org.rakam.presto.stream.metadata;

import com.google.inject.Binder;
import com.google.inject.Provides;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.dbpool.H2EmbeddedDataSourceModule;
import io.airlift.dbpool.MySqlDataSourceModule;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Singleton;
import javax.sql.DataSource;
import java.lang.annotation.Annotation;

import static org.rakam.presto.stream.metadata.ConditionalModule.installIfPropertyEquals;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/01/15 13:23.
 */
public class DatabaseMetadataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindDataSource("metadata", ForMetadata.class);
    }

    @ForMetadata
    @Singleton
    @Provides
    public ConnectionFactory createConnectionFactory(@ForMetadata DataSource dataSource)
    {
        return dataSource::getConnection;
    }

    private void bindDataSource(String type, Class<? extends Annotation> annotation)
    {
        String property = type + ".db.type";
        install(installIfPropertyEquals(new MySqlDataSourceModule(type, annotation), property, "mysql"));
        install(installIfPropertyEquals(new H2EmbeddedDataSourceModule(type, annotation), property, "h2"));
    }
}
