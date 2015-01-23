package org.rakam.presto.stream;

import com.google.inject.Module;
import org.rakam.presto.stream.metadata.DatabaseMetadataModule;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/01/15 14:10.
 */
public class PluginInfo
{
    public String getName()
    {
        return "stream";
    }

    public Module getModule()
    {
        return new DatabaseMetadataModule();
    }
}
