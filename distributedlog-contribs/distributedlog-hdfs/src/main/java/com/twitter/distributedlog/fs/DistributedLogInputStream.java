package com.twitter.distributedlog.fs;

import com.google.common.base.Objects;
import com.twitter.distributedlog.AppendOnlyStreamReader;
import com.twitter.distributedlog.DistributedLogManager;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;

/**
 * The input stream for a distributedlog stream.
 */
class DistributedLogInputStream extends FSInputStream {

    private final DistributedLogManager dlm;
    private final AppendOnlyStreamReader streamReader;

    DistributedLogInputStream(DistributedLogManager dlm,
                              AppendOnlyStreamReader streamReader) throws IOException {
        this.dlm = dlm;
        this.streamReader = streamReader;
    }

    //
    // FSInputStream
    //

    @Override
    public void seek(long pos) throws IOException {
        this.streamReader.skipTo(pos);
    }

    @Override
    public long getPos() throws IOException {
        return this.streamReader.position();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    //
    // Input Stream
    //


    @Override
    public int read(byte[] b) throws IOException {
        return streamReader.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return streamReader.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return streamReader.skip(n);
    }

    @Override
    public int available() throws IOException {
        return streamReader.available();
    }

    @Override
    public void close() throws IOException {
        streamReader.close();
        dlm.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        streamReader.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        streamReader.reset();
    }

    @Override
    public boolean markSupported() {
        return streamReader.markSupported();
    }

    @Override
    public int read() throws IOException {
        return streamReader.read();
    }

    //
    // Object
    //


    @Override
    public int hashCode() {
        return streamReader.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DistributedLogInputStream)) {
            return false;
        }
        DistributedLogInputStream another = (DistributedLogInputStream) obj;
        return Objects.equal(streamReader, another.streamReader);
    }

    @Override
    public String toString() {
        return streamReader.toString();
    }
}
