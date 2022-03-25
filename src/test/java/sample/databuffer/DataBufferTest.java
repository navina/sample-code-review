package sample.databuffer;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DataBufferTest {
  private static final Random RANDOM = new Random();
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(10);
  private static final File TEMP_FILE = new File(FileUtils.getTempDirectory(), "DataBufferTest");
  private static final int FILE_OFFSET = 10;      // Not page-aligned
  private static final int BUFFER_SIZE = 10_000;  // Not page-aligned
  private static final int CHAR_ARRAY_LENGTH = BUFFER_SIZE / Character.BYTES;
  private static final int SHORT_ARRAY_LENGTH = BUFFER_SIZE / Short.BYTES;
  private static final int INT_ARRAY_LENGTH = BUFFER_SIZE / Integer.BYTES;
  private static final int LONG_ARRAY_LENGTH = BUFFER_SIZE / Long.BYTES;
  private static final int FLOAT_ARRAY_LENGTH = BUFFER_SIZE / Float.BYTES;
  private static final int DOUBLE_ARRAY_LENGTH = BUFFER_SIZE / Double.BYTES;
  private static final int NUM_ROUNDS = 1000;
  private static final int MAX_BYTES_LENGTH = 100;

  private byte[] _bytes = new byte[BUFFER_SIZE];
  private char[] _chars = new char[CHAR_ARRAY_LENGTH];
  private short[] _shorts = new short[SHORT_ARRAY_LENGTH];
  private int[] _ints = new int[INT_ARRAY_LENGTH];
  private long[] _longs = new long[LONG_ARRAY_LENGTH];
  private float[] _floats = new float[FLOAT_ARRAY_LENGTH];
  private double[] _doubles = new double[DOUBLE_ARRAY_LENGTH];

  @BeforeClass
  public void setUp() {
    for (int i = 0; i < BUFFER_SIZE; i++) {
      _bytes[i] = (byte) RANDOM.nextInt();
    }
    for (int i = 0; i < CHAR_ARRAY_LENGTH; i++) {
      _chars[i] = (char) RANDOM.nextInt();
    }
    for (int i = 0; i < SHORT_ARRAY_LENGTH; i++) {
      _shorts[i] = (short) RANDOM.nextInt();
    }
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      _ints[i] = RANDOM.nextInt();
    }
    for (int i = 0; i < LONG_ARRAY_LENGTH; i++) {
      _longs[i] = RANDOM.nextLong();
    }
    for (int i = 0; i < FLOAT_ARRAY_LENGTH; i++) {
      _floats[i] = RANDOM.nextFloat();
    }
    for (int i = 0; i < DOUBLE_ARRAY_LENGTH; i++) {
      _doubles[i] = RANDOM.nextDouble();
    }
  }

  @Test
  public void testBBuffer()
      throws Exception {
    try (DataBuffer buffer = BBuffer.allocateDirect(BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
      testDataBuffer(buffer);
    }
    try (DataBuffer buffer = BBuffer.allocateDirect(BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
      testDataBuffer(buffer);
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (DataBuffer buffer = BBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
        testDataBuffer(buffer);
      }
      try (DataBuffer buffer = BBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
        testDataBuffer(buffer);
      }
      try (DataBuffer buffer = BBuffer
          .mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
        testDataBuffer(buffer);
      }
      try (DataBuffer buffer = BBuffer
          .mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
        testDataBuffer(buffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  private void testDataBuffer(DataBuffer buffer)
      throws Exception {
    Assert.assertEquals(buffer.size(), BUFFER_SIZE);
    testReadWriteByte(buffer);
    testReadWriteChar(buffer);
    testReadWriteInt(buffer);
    testReadWriteLong(buffer);
    testReadWriteBytes(buffer);
    testReadWriteDataBuffer(buffer);
    testConcurrentReadWrite(buffer);
  }

  private void testReadWriteByte(DataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int intOffset = RANDOM.nextInt(BUFFER_SIZE);
      buffer.putByte(intOffset, _bytes[i]);
      Assert.assertEquals(buffer.getByte(intOffset), _bytes[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      long longOffset = RANDOM.nextInt(BUFFER_SIZE);
      buffer.putByte(longOffset, _bytes[i]);
      Assert.assertEquals(buffer.getByte(longOffset), _bytes[i]);
    }
  }

  private void testReadWriteChar(DataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(CHAR_ARRAY_LENGTH);
      int intOffset = index * Byte.BYTES;
      buffer.putChar(intOffset, _chars[i]);
      Assert.assertEquals(buffer.getChar(intOffset), _chars[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(CHAR_ARRAY_LENGTH);
      long longOffset = index * Byte.BYTES;
      buffer.putChar(longOffset, _chars[i]);
      Assert.assertEquals(buffer.getChar(longOffset), _chars[i]);
    }
  }


  private void testReadWriteInt(DataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(INT_ARRAY_LENGTH);
      int intOffset = index * Byte.BYTES;
      buffer.putInt(intOffset, _ints[i]);
      Assert.assertEquals(buffer.getInt(intOffset), _ints[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(INT_ARRAY_LENGTH);
      long longOffset = index * Byte.BYTES;
      buffer.putInt(longOffset, _ints[i]);
      Assert.assertEquals(buffer.getInt(longOffset), _ints[i]);
    }
  }

  private void testReadWriteLong(DataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(LONG_ARRAY_LENGTH);
      int intOffset = index * Byte.BYTES;
      buffer.putLong(intOffset, _longs[i]);
      Assert.assertEquals(buffer.getLong(intOffset), _longs[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(LONG_ARRAY_LENGTH);
      long longOffset = index * Byte.BYTES;
      buffer.putLong(longOffset, _longs[i]);
      Assert.assertEquals(buffer.getLong(longOffset), _longs[i]);
    }
  }


  private void testReadWriteBytes(DataBuffer buffer) {
    byte[] readBuffer = new byte[MAX_BYTES_LENGTH];
    byte[] writeBuffer = new byte[MAX_BYTES_LENGTH];
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int length = RANDOM.nextInt(MAX_BYTES_LENGTH);
      int offset = RANDOM.nextInt(BUFFER_SIZE - length);
      int arrayOffset = RANDOM.nextInt(MAX_BYTES_LENGTH - length);
      System.arraycopy(_bytes, offset, readBuffer, arrayOffset, length);
      buffer.doSomethingElse(offset, readBuffer, arrayOffset, length);
      buffer.doSomething(offset, writeBuffer, arrayOffset, length);
      int end = arrayOffset + length;
      for (int j = arrayOffset; j < end; j++) {
        Assert.assertEquals(writeBuffer[j], readBuffer[j]);
      }
    }
  }

  private void testReadWriteDataBuffer(DataBuffer buffer) {
    testReadWriteDataBuffer(buffer, BBuffer.allocateDirect(MAX_BYTES_LENGTH, DataBuffer.NATIVE_ORDER),
        BBuffer.allocateDirect(MAX_BYTES_LENGTH, DataBuffer.NON_NATIVE_ORDER));
  }

  private void testReadWriteDataBuffer(DataBuffer buffer, DataBuffer readBuffer,
      DataBuffer writeBuffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int length = RANDOM.nextInt(MAX_BYTES_LENGTH);
      int offset = RANDOM.nextInt(BUFFER_SIZE - length);
      readBuffer.doSomethingElse(0, _bytes, RANDOM.nextInt(BUFFER_SIZE - length), length);
      readBuffer.doSomething(0, buffer, offset, length);
      buffer.doSomething(offset, writeBuffer, 0, length);
      for (int j = 0; j < length; j++) {
        Assert.assertEquals(writeBuffer.getByte(j), readBuffer.getByte(j));
      }
    }
  }

  private void testConcurrentReadWrite(DataBuffer buffer)
      throws Exception {
    Future[] futures = new Future[NUM_ROUNDS];
    for (int i = 0; i < NUM_ROUNDS; i++) {
      futures[i] = EXECUTOR_SERVICE.submit(() -> {
        int length = RANDOM.nextInt(MAX_BYTES_LENGTH);
        int offset = RANDOM.nextInt(BUFFER_SIZE - length);
        byte[] readBuffer = new byte[length];
        byte[] writeBuffer = new byte[length];
        System.arraycopy(_bytes, offset, readBuffer, 0, length);
        buffer.doSomethingElse(offset, readBuffer);
        buffer.doSomething(offset, writeBuffer);
        Assert.assertTrue(Arrays.equals(readBuffer, writeBuffer));
        buffer.doSomethingElse(offset, ByteBuffer.wrap(readBuffer));
        buffer.doSomething(offset, writeBuffer);
        Assert.assertTrue(Arrays.equals(readBuffer, writeBuffer));
      });
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      futures[i].get();
    }
  }

  @Test
  public void testBBufferReadWriteFile()
      throws Exception {
    try (DataBuffer writeBuffer = BBuffer
        .mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, DataBuffer.NATIVE_ORDER)) {
      putInts(writeBuffer);
      try (DataBuffer readBuffer = BBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, DataBuffer.NATIVE_ORDER)) {
        getInts(readBuffer);
      }
      try (DataBuffer readBuffer = BBuffer
          .mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE, DataBuffer.NATIVE_ORDER)) {
        getInts(readBuffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  private void putInts(DataBuffer buffer) {
    Assert.assertEquals(buffer.size(), BUFFER_SIZE);
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      buffer.putInt(i * Integer.BYTES, _ints[i]);
    }
    buffer.flush();
  }

  private void getInts(DataBuffer buffer) {
    Assert.assertEquals(buffer.size(), BUFFER_SIZE);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(INT_ARRAY_LENGTH);
      Assert.assertEquals(buffer.getInt(index * Integer.BYTES), _ints[index]);
    }
  }

  @AfterClass
  public void tearDown() {
    EXECUTOR_SERVICE.shutdown();
  }
}
