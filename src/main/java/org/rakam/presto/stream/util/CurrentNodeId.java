package org.rakam.presto.stream.util;

import static com.google.common.base.Preconditions.checkNotNull;

public class CurrentNodeId
{
    private final String id;

    public CurrentNodeId(String id)
    {
        this.id = checkNotNull(id, "id is null");
    }

    @Override
    public String toString()
    {
        return id;
    }
}
