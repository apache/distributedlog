package com.twitter.distributedlog.fs;

import com.twitter.distributedlog.AppendOnlyStreamWriter;
import com.twitter.distributedlog.DistributedLogManager;

import java.io.IOException;
import java.io.OutputStream;

/**
 * DistributedLog Output Stream
 */
class DistributedLogOutputStream extends OutputStream {

    private final DistributedLogManager dlm;
    private final AppendOnlyStreamWriter streamWriter;

    DistributedLogOutputStream(DistributedLogManager dlm,
                               AppendOnlyStreamWriter streamWriter) {
        this.dlm = dlm;
        this.streamWriter = streamWriter;
    }

    @Override
    public void write(int b) throws IOException {
        byte[] data = new byte[] { (byte) b };
        write(data);
    }

    @Override
    public void write(byte[] b) throws IOException {
        streamWriter.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // TODO: improve AppendOnlyStreamWriter
        byte[] newData = new byte[len];
        System.arraycopy(b, off, newData, 0, len);
        streamWriter.write(newData);
    }

    @Override
    public void flush() throws IOException {
        streamWriter.force(false);
    }

    @Override
    public void close() throws IOException {
        streamWriter.close();
        dlm.close();
    }
}
