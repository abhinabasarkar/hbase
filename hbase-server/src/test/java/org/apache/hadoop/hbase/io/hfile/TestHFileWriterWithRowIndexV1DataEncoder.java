package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Category({ IOTests.class, MediumTests.class})
public class TestHFileWriterWithRowIndexV1DataEncoder {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileWriterWithRowIndexV1DataEncoder.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestHFileWriterWithRowIndexV1DataEncoder.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Configuration conf;
  private FileSystem fs;
  private DataBlockEncoding dataBlockEncoding;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    fs = FileSystem.get(conf);
    dataBlockEncoding = DataBlockEncoding.ROW_INDEX_V1;
  }

  @Test
  public void testBlockCountWritten() throws IOException {
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(), "testHFileFormatV3");
    final int entryCount = 1000;
    writeDataAndReadFromHFile(hfilePath, entryCount);
  }

  private void writeDataAndReadFromHFile(Path hfilePath, int entryCount) throws IOException {
    HFileContext context = new HFileContextBuilder()
      .withBlockSize(1024)
      .withDataBlockEncoding(dataBlockEncoding)
      .withCellComparator(CellComparatorImpl.COMPARATOR).build();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HFile.Writer writer = new HFile.WriterFactory(conf, cacheConfig)
      .withPath(fs, hfilePath)
      .withFileContext(context)
      .create();

    List<KeyValue> keyValues = new ArrayList<>(entryCount);

    writeKeyValues(entryCount, writer, keyValues);

    FSDataInputStream fsdis = fs.open(hfilePath);

    long fileSize = fs.getFileStatus(hfilePath).getLen();
    FixedFileTrailer trailer =
      FixedFileTrailer.readFromStream(fsdis, fileSize);

    Assert.assertEquals(3, trailer.getMajorVersion());
    Assert.assertEquals(entryCount, trailer.getEntryCount());
    System.out.println(trailer.getDataIndexCount());
  }

  private void writeKeyValues(int entryCount, HFile.Writer writer, List<KeyValue> keyValues) throws IOException {
    for (int i = 0; i < entryCount; ++i) {
      byte[] keyBytes = intToBytes(i);

      // A random-length random value.
      byte[] valueBytes = new byte[0];
      KeyValue keyValue = new KeyValue(keyBytes, null, null, valueBytes);

      writer.append(keyValue);
      keyValues.add(keyValue);
    }
    writer.close();
  }

  private byte[] intToBytes( final int i ) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(i);
    return bb.array();
  }
}
