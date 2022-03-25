package sample.databuffer;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>sample.databuffer.DataBuffer</code> is a byte buffer data that resides in off-heap memory.
 * The byte buffer may be memory mapped (MMAP) or direct allocated (DIRECT).
 */
@ThreadSafe
public abstract class DataBuffer implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataBuffer.class);

  public static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();
  public static final ByteOrder NON_NATIVE_ORDER =
      NATIVE_ORDER == ByteOrder.BIG_ENDIAN ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
  // We use this threshold to decide whether we use bulk bytes processing or not
  // With number of bytes less than this threshold, we get/put bytes one by one
  // With number of bytes more than this threshold, we create a ByteBuffer from the buffer and use bulk get/put method
  public static final int BULK_BYTES_PROCESSING_THRESHOLD = 10;

  private static class BufferContext {
    enum Type {
      DIRECT, MMAP
    }

    final Type _type;
    final long _size;
    final String _filePath;
    final String _description;

    BufferContext(Type type, long size, @Nullable String filePath, @Nullable String description) {
      _type = type;
      _size = size;
      _filePath = filePath;
      _description = description;
    }

    @Override
    public String toString() {
      String context = "Type: " + _type + ", Size: " + _size;
      if (_filePath != null) {
        context += ", File Path: " + _filePath;
      }
      if (_description != null) {
        context += ", Description: " + _description;
      }
      return context;
    }
  }

  private static final AtomicLong DIRECT_BUFFER_COUNT = new AtomicLong();
  private static final AtomicLong DIRECT_BUFFER_USAGE = new AtomicLong();
  private static final AtomicLong MMAP_BUFFER_COUNT = new AtomicLong();
  private static final AtomicLong MMAP_BUFFER_USAGE = new AtomicLong();
  private static final AtomicLong ALLOCATION_FAILURE_COUNT = new AtomicLong();
  private static final Map<DataBuffer, BufferContext> BUFFER_CONTEXT_MAP = new WeakHashMap<>();

  /**
   * Allocates a buffer using direct memory.
   * <p>NOTE: The contents of the allocated buffer are not defined.
   *
   * @param size The size of the buffer
   * @param byteOrder The byte order of the buffer (big-endian or little-endian)
   * @param description The description of the buffer
   * @return The buffer allocated
   */
  public static DataBuffer allocateDirect(int size, ByteOrder byteOrder, @Nullable String description) {
    DataBuffer buffer;
    try {
        buffer = BBuffer.allocateDirect(size, byteOrder);
    } catch (Exception e) {
      LOGGER
          .error("Caught exception while allocating direct buffer of size: {} with description: {}", size, description,
              e);
      LOGGER.error("Buffer stats: {}", getBufferStats());
      ALLOCATION_FAILURE_COUNT.getAndIncrement();
      throw e;
    }
    DIRECT_BUFFER_COUNT.getAndIncrement();
    DIRECT_BUFFER_USAGE.getAndAdd(size);
    synchronized (BUFFER_CONTEXT_MAP) {
      BUFFER_CONTEXT_MAP.put(buffer, new BufferContext(BufferContext.Type.DIRECT, size, null, description));
    }
    return buffer;
  }

  /**
   * Allocates a buffer using direct memory and loads a file into the buffer.
   */
  public static DataBuffer loadFile(File file, long offset, int size, ByteOrder byteOrder,
      @Nullable String description)
      throws IOException {
    DataBuffer buffer;
    try {
        buffer = BBuffer.loadFile(file, offset, size, byteOrder);
    } catch (Exception e) {
      LOGGER.error("Caught exception while loading file: {} from offset: {} of size: {} with description: {}",
          file.getAbsolutePath(), offset, size, description, e);
      LOGGER.error("Buffer stats: {}", getBufferStats());
      ALLOCATION_FAILURE_COUNT.getAndIncrement();
      throw e;
    }
    DIRECT_BUFFER_COUNT.getAndIncrement();
    DIRECT_BUFFER_USAGE.getAndAdd(size);
    synchronized (BUFFER_CONTEXT_MAP) {
      BUFFER_CONTEXT_MAP.put(buffer,
          new BufferContext(BufferContext.Type.DIRECT, size, file.getAbsolutePath().intern(), description));
    }
    return buffer;
  }

  /**
   * Allocates a buffer using direct memory and loads a big-endian file into the buffer.
   */
  @VisibleForTesting
  public static DataBuffer loadBigEndianFile(File file)
      throws IOException {
    return loadFile(file, 0, (int) file.length(), ByteOrder.BIG_ENDIAN, null);
  }

  /**
   * Memory maps a file into a buffer.
   * <p>NOTE: If the file gets extended, the contents of the extended portion of the file are not defined.
   */
  public static DataBuffer mapFile(File file, boolean readOnly, long offset, int size, ByteOrder byteOrder,
      @Nullable String description)
      throws IOException {
    DataBuffer buffer;
    try {
        buffer = BBuffer.mapFile(file, readOnly, offset, (int) size, byteOrder);
    } catch (Exception e) {
      LOGGER.error("Caught exception while mapping file: {} from offset: {} of size: {} with description: {}",
          file.getAbsolutePath(), offset, size, description, e);
      LOGGER.error("Buffer stats: {}", getBufferStats());
      ALLOCATION_FAILURE_COUNT.getAndIncrement();
      throw e;
    }
    MMAP_BUFFER_COUNT.getAndIncrement();
    MMAP_BUFFER_USAGE.getAndAdd(size);
    synchronized (BUFFER_CONTEXT_MAP) {
      BUFFER_CONTEXT_MAP
          .put(buffer, new BufferContext(BufferContext.Type.MMAP, size, file.getAbsolutePath().intern(), description));
    }
    return buffer;
  }

  /**
   * Memory maps a read-only big-endian file into a buffer.
   */
  @VisibleForTesting
  public static DataBuffer mapReadOnlyBigEndianFile(File file)
      throws IOException {
    return mapFile(file, true, 0, (int) file.length(), ByteOrder.BIG_ENDIAN, null);
  }

  public static long getDirectBufferCount() {
    return DIRECT_BUFFER_COUNT.get();
  }

  public static long getDirectBufferUsage() {
    return DIRECT_BUFFER_USAGE.get();
  }

  public static long getMmapBufferCount() {
    return MMAP_BUFFER_COUNT.get();
  }

  public static long getMmapBufferUsage() {
    return MMAP_BUFFER_USAGE.get();
  }

  public static long getAllocationFailureCount() {
    return ALLOCATION_FAILURE_COUNT.get();
  }

  public static List<String> getBufferInfo() {
    synchronized (BUFFER_CONTEXT_MAP) {
      List<String> bufferInfo = new ArrayList<>(BUFFER_CONTEXT_MAP.size());
      for (BufferContext bufferContext : BUFFER_CONTEXT_MAP.values()) {
        bufferInfo.add(bufferContext.toString());
      }
      return bufferInfo;
    }
  }

  private static String getBufferStats() {
    return String
        .format("Direct buffer count: %s, size: %s; Mmap buffer count: %s, size: %s", DIRECT_BUFFER_COUNT.get(),
            DIRECT_BUFFER_USAGE.get(), MMAP_BUFFER_COUNT.get(), MMAP_BUFFER_USAGE.get());
  }

  private boolean _closeable;

  protected DataBuffer(boolean closeable) {
    _closeable = closeable;
  }

  @Override
  public synchronized void close()
      throws IOException {
    if (_closeable) {
      flush();
      BufferContext bufferContext;
      synchronized (BUFFER_CONTEXT_MAP) {
        bufferContext = BUFFER_CONTEXT_MAP.remove(this);
      }
      if (bufferContext != null) {
        if (bufferContext._type == BufferContext.Type.DIRECT) {
          DIRECT_BUFFER_COUNT.getAndDecrement();
          DIRECT_BUFFER_USAGE.getAndAdd(-bufferContext._size);
        } else {
          MMAP_BUFFER_COUNT.getAndDecrement();
          MMAP_BUFFER_USAGE.getAndAdd(-bufferContext._size);
        }
      }
      _closeable = false;
    }
  }

  public abstract byte getByte(int offset);

  public abstract byte getByte(long offset);

  public abstract void putByte(int offset, byte value);

  public abstract void putByte(long offset, byte value);

  public abstract char getChar(int offset);

  public abstract char getChar(long offset);

  public abstract void putChar(int offset, char value);

  public abstract void putChar(long offset, char value);

  public abstract short getShort(int offset);

  public abstract short getShort(long offset);

  public abstract void putShort(int offset, short value);

  public abstract void putShort(long offset, short value);

  public abstract int getInt(int offset);

  public abstract int getInt(long offset);

  public abstract void putInt(int offset, int value);

  public abstract void putInt(long offset, int value);

  public abstract long getLong(int offset);

  public abstract long getLong(long offset);

  public abstract void putLong(int offset, long value);

  public abstract void putLong(long offset, long value);

  public abstract float getFloat(int offset);

  public abstract float getFloat(long offset);

  public abstract void putFloat(int offset, float value);

  public abstract void putFloat(long offset, float value);

  public abstract double getDouble(int offset);

  public abstract double getDouble(long offset);

  public abstract void putDouble(int offset, double value);

  public abstract void putDouble(long offset, double value);

  public abstract void copyTo(long offset, byte[] buffer, int destOffset, int size);

  public void copyTo(long offset, byte[] buffer) {
    copyTo(offset, buffer, 0, buffer.length);
  }

  public abstract void copyTo(long offset, DataBuffer buffer, long destOffset, long size);

  public abstract void readFrom(long offset, byte[] buffer, int srcOffset, int size);

  public void readFrom(long offset, byte[] buffer) {
    readFrom(offset, buffer, 0, buffer.length);
  }

  public abstract void readFrom(long offset, ByteBuffer buffer);

  public abstract void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException;

  public abstract long size();

  public abstract ByteOrder order();

  public abstract ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder);

  public ByteBuffer toDirectByteBuffer(long offset, int size) {
    return toDirectByteBuffer(offset, size, order());
  }

  public abstract void flush();
}
