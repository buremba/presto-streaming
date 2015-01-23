package org.rakam.presto.stream;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/01/15 10:54.
 */
public enum StreamErrorCode implements ErrorCodeSupplier {
    STREAM_ERROR(0x0300_0000);

    private final ErrorCode errorCode;

    StreamErrorCode(int code)
    {
        errorCode = new ErrorCode(code, name());
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
