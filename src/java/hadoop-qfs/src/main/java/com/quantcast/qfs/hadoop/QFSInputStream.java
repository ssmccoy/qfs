/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * Implements the Hadoop FSInputStream interfaces to allow applications to read
 * files in Quantcast File System (QFS), an extension of KFS.
 */

package com.quantcast.qfs.hadoop;

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.util.StringUtils;

import com.quantcast.qfs.access.KfsAccess;
import com.quantcast.qfs.access.KfsInputChannel;

class QFSInputStream extends FSInputStream
implements StreamCapabilities, ByteBufferReadable, CanUnbuffer, CanSetReadahead
{
  private final KfsInputChannel kfsChannel;
  private FileSystem.Statistics statistics;
  private final long fsize;

  public QFSInputStream(KfsAccess kfsAccess, String path,
                        FileSystem.Statistics stats) throws IOException {
    this.statistics = stats;
    this.kfsChannel = kfsAccess.kfs_open_ex(path, -1, -1);
    if (kfsChannel == null) {
      throw new IOException("QFS internal error -- null channel");
    }
    this.fsize = kfsAccess.kfs_filesize(path);
    if (this.fsize < 0) {
        kfsAccess.kfs_retToIOException((int)this.fsize);
    }
  }

  @Override
  public boolean hasCapability(final String capability) {
    switch (StringUtils.toLowerCase(capability)) {
      case StreamCapabilities.UNBUFFER:
      case StreamCapabilities.READBYTEBUFFER:
      case StreamCapabilities.READAHEAD:
        return true;
      case StreamCapabilities.PREADBYTEBUFFER:
      case StreamCapabilities.DROPBEHIND:
      default:
        return false;
    }
  }

  @Override
  public long getPos() throws IOException {
    if (kfsChannel == null) {
      throw new IOException("File closed");
    }
    return kfsChannel.tell();
  }

  @Override
  public int available() throws IOException {
    return (int) (this.fsize - getPos());
  }

  @Override
  public void seek(long targetPos) throws IOException {
    kfsChannel.seek(targetPos);
  }

  @Override
  public boolean seekToNewSource(long targetPos)
    throws IOException {
    return false;
  }

  @Override
  public int read(final ByteBuffer buffer)
  throws IOException {
    return kfsChannel.read(buffer);
  }

  @Override
  public int read() throws IOException {
    int c = kfsChannel.read();

    statistics.incrementBytesRead(1);

    return c;
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    final int res = kfsChannel.read(ByteBuffer.wrap(b, off, len));
    // Use -1 to signify EOF
    if (res == 0) {
        return -1;
    }
    if (statistics != null) {
      statistics.incrementBytesRead(res);
    }
    return res;
  }

  @Override
  public void unbuffer() {
    kfsChannel.unbuffer();
  }

  @Override
  public void close() throws IOException {
    kfsChannel.close();
  }

  @Override
  public void setReadahead(Long readAheadSize) {
    kfsChannel.setReadAheadSize(readAheadSize);
  }
}
