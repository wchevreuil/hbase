/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.BuilderStyleTest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PrettyPrinter;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import java.util.Map;

@Category({MiscTests.class, SmallTests.class})
public class TestColumnFamilyDescriptorBuilder {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestColumnFamilyDescriptorBuilder.class);

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testBuilder() throws DeserializationException {
    ColumnFamilyDescriptorBuilder builder
      = ColumnFamilyDescriptorBuilder.newBuilder(HConstants.CATALOG_FAMILY)
            .setInMemory(true)
            .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
            .setBloomFilterType(BloomType.NONE);
    final int v = 123;
    builder.setBlocksize(v);
    builder.setTimeToLive(v);
    builder.setBlockCacheEnabled(!HColumnDescriptor.DEFAULT_BLOCKCACHE);
    builder.setValue(Bytes.toBytes("a"), Bytes.toBytes("b"));
    builder.setMaxVersions(v);
    assertEquals(v, builder.build().getMaxVersions());
    builder.setMinVersions(v);
    assertEquals(v, builder.build().getMinVersions());
    builder.setKeepDeletedCells(KeepDeletedCells.TRUE);
    builder.setInMemory(!HColumnDescriptor.DEFAULT_IN_MEMORY);
    boolean inmemory = builder.build().isInMemory();
    builder.setScope(v);
    builder.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    builder.setBloomFilterType(BloomType.ROW);
    builder.setCompressionType(Algorithm.SNAPPY);
    builder.setMobEnabled(true);
    builder.setMobThreshold(1000L);
    builder.setDFSReplication((short) v);

    ColumnFamilyDescriptor hcd = builder.build();
    byte [] bytes = ColumnFamilyDescriptorBuilder.toByteArray(hcd);
    ColumnFamilyDescriptor deserializedHcd = ColumnFamilyDescriptorBuilder.parseFrom(bytes);
    assertTrue(hcd.equals(deserializedHcd));
    assertEquals(v, hcd.getBlocksize());
    assertEquals(v, hcd.getTimeToLive());
    assertTrue(Bytes.equals(hcd.getValue(Bytes.toBytes("a")),
        deserializedHcd.getValue(Bytes.toBytes("a"))));
    assertEquals(hcd.getMaxVersions(), deserializedHcd.getMaxVersions());
    assertEquals(hcd.getMinVersions(), deserializedHcd.getMinVersions());
    assertEquals(hcd.getKeepDeletedCells(), deserializedHcd.getKeepDeletedCells());
    assertEquals(inmemory, deserializedHcd.isInMemory());
    assertEquals(hcd.getScope(), deserializedHcd.getScope());
    assertTrue(deserializedHcd.getCompressionType().equals(Compression.Algorithm.SNAPPY));
    assertTrue(deserializedHcd.getDataBlockEncoding().equals(DataBlockEncoding.FAST_DIFF));
    assertTrue(deserializedHcd.getBloomFilterType().equals(BloomType.ROW));
    assertEquals(hcd.isMobEnabled(), deserializedHcd.isMobEnabled());
    assertEquals(hcd.getMobThreshold(), deserializedHcd.getMobThreshold());
    assertEquals(v, deserializedHcd.getDFSReplication());
  }

  /**
   * Tests HColumnDescriptor with empty familyName
   */
  @Test
  public void testHColumnDescriptorShouldThrowIAEWhenFamilyNameEmpty() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Column Family name can not be empty");
    ColumnFamilyDescriptorBuilder.of("");
  }

  /**
   * Test that we add and remove strings from configuration properly.
   */
  @Test
  public void testAddGetRemoveConfiguration() {
    ColumnFamilyDescriptorBuilder builder
      = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("foo"));
    String key = "Some";
    String value = "value";
    builder.setConfiguration(key, value);
    assertEquals(value, builder.build().getConfigurationValue(key));
    builder.removeConfiguration(key);
    assertEquals(null, builder.build().getConfigurationValue(key));
  }

  @Test
  public void testMobValuesInHColumnDescriptorShouldReadable() {
    boolean isMob = true;
    long threshold = 1000;
    String policy = "weekly";
    // We unify the format of all values saved in the descriptor.
    // Each value is stored as bytes of string.
    String isMobString = PrettyPrinter.format(String.valueOf(isMob),
            HColumnDescriptor.getUnit(HColumnDescriptor.IS_MOB));
    String thresholdString = PrettyPrinter.format(String.valueOf(threshold),
            HColumnDescriptor.getUnit(HColumnDescriptor.MOB_THRESHOLD));
    String policyString = PrettyPrinter.format(Bytes.toStringBinary(Bytes.toBytes(policy)),
        HColumnDescriptor.getUnit(HColumnDescriptor.MOB_COMPACT_PARTITION_POLICY));
    assertEquals(String.valueOf(isMob), isMobString);
    assertEquals(String.valueOf(threshold), thresholdString);
    assertEquals(String.valueOf(policy), policyString);
  }

  @Test
  public void testClassMethodsAreBuilderStyle() {
    /* HColumnDescriptor should have a builder style setup where setXXX/addXXX methods
     * can be chainable together:
     * . For example:
     * HColumnDescriptor hcd
     *   = new HColumnDescriptor()
     *     .setFoo(foo)
     *     .setBar(bar)
     *     .setBuz(buz)
     *
     * This test ensures that all methods starting with "set" returns the declaring object
     */

    BuilderStyleTest.assertClassesAreBuilderStyle(ColumnFamilyDescriptorBuilder.class);
  }

  @Test
  public void testSetTimeToLive() throws HBaseException {
    String ttl;
    ColumnFamilyDescriptorBuilder builder
      = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("foo"));

    ttl = "50000";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(50000, builder.build().getTimeToLive());

    ttl = "50000 seconds";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(50000, builder.build().getTimeToLive());

    ttl = "";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(0, builder.build().getTimeToLive());

    ttl = "FOREVER";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(HConstants.FOREVER, builder.build().getTimeToLive());

    ttl = "1 HOUR 10 minutes 1 second";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(4201, builder.build().getTimeToLive());

    ttl = "500 Days 23 HOURS";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(43282800, builder.build().getTimeToLive());

    ttl = "43282800 SECONDS (500 Days 23 hours)";
    builder.setTimeToLive(ttl);
    Assert.assertEquals(43282800, builder.build().getTimeToLive());
  }

  /**
   * Test for verifying the ColumnFamilyDescriptorBuilder's default values so that backward
   * compatibility with hbase-1.x can be mantained (see HBASE-24981).
   */
  @Test
  public void testDefaultBuilder() {
    final Map<String, String> defaultValueMap = ColumnFamilyDescriptorBuilder.getDefaultValues();
    assertEquals(defaultValueMap.size(), 12);
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.BLOOMFILTER),
      BloomType.ROW.toString());
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.REPLICATION_SCOPE), "0");
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.MAX_VERSIONS), "1");
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.MIN_VERSIONS), "0");
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.COMPRESSION),
      Compression.Algorithm.NONE.toString());
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.TTL),
      Integer.toString(Integer.MAX_VALUE));
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.BLOCKSIZE),
      Integer.toString(64 * 1024));
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.IN_MEMORY),
      Boolean.toString(false));
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.BLOCKCACHE),
      Boolean.toString(true));
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS),
      KeepDeletedCells.FALSE.toString());
    assertEquals(defaultValueMap.get(ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING),
      DataBlockEncoding.NONE.toString());

  }

  // See CDPD-17676. Under C5 and HDP2.6 we stored IS_MOB value as a boolean,
  // under C6 it became a string of "true" or "false".
  @Test
  public void testIsMobEnabled() {
    ColumnFamilyDescriptorBuilder builder
        = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("mob"));
    byte[] isMob = Bytes.toBytes(ColumnFamilyDescriptorBuilder.IS_MOB);

    builder.setValue(isMob, Bytes.toBytes(false));
    Assert.assertFalse(builder.build().isMobEnabled());

    builder.setValue(isMob, Bytes.toBytes(true));
    Assert.assertTrue(builder.build().isMobEnabled());

    builder.setMobEnabled(false);
    Assert.assertFalse(builder.build().isMobEnabled());

    builder.setMobEnabled(true);
    Assert.assertTrue(builder.build().isMobEnabled());

    builder.setValue(isMob, Bytes.toBytes("false"));
    Assert.assertFalse(builder.build().isMobEnabled());

    builder.setValue(isMob, Bytes.toBytes("true"));
    Assert.assertTrue(builder.build().isMobEnabled());

    builder.setValue(isMob, Bytes.toBytes("unknown"));
    Assert.assertFalse(builder.build().isMobEnabled());
  }

  /**
   * See CDPD-21045. Under C5 and HDP2.6 we stored MOB_THRESHOLD as a long, under later versions it
   * is a string of the number.
   */
  @Test
  public void testMobThreshold() {
    ColumnFamilyDescriptorBuilder builder
        = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("mob"));
    byte[] threshold = Bytes.toBytes(ColumnFamilyDescriptorBuilder.MOB_THRESHOLD);

    // unambiguous shorter than long
    builder.setValue(threshold, Bytes.toBytes("100000"));
    Assert.assertEquals("string that is unambiguous due to length parsed incorrectly.", 100000l,
        builder.build().getMobThreshold());

    // unambiguous longer than long
    builder.setValue(threshold, Bytes.toBytes("100000000"));
    Assert.assertEquals("string that is unambiguous due to length parsed incorrectly.",
        100000000l, builder.build().getMobThreshold());

    // ambiguous strings
    builder.setValue(threshold, Bytes.toBytes("10000000"));
    Assert.assertEquals("string that is ambiguous length parsed incorrectly.",
        10000000l, builder.build().getMobThreshold());

    // long w/only invalid character sequences (this is the invalid byte 0xC0 repeated)
    // the default replacement character '0xfffd' isn't a digit so it should fail to parse and
    // get handled as a serialized long.
    builder.setValue(threshold, Bytes.toBytes(0xC0C0C0C0C0C0C0C0l));
    Assert.assertEquals("long that does not parse as a string of numbers parsed incorrectly.",
        0xC0C0C0C0C0C0C0C0l, builder.build().getMobThreshold());

    // long w/some digit string bytes and some non-digit string bytes
    builder.setValue(threshold, Bytes.toBytes(0x303030l));
    Assert.assertEquals("long that does not parse as a string of numbers parsed incorrectly.",
        0x303030l, builder.build().getMobThreshold());

    // long w/only non-digit string representation
    builder.setValue(threshold, Bytes.toBytes(102400l));
    Assert.assertEquals("long that does not parse as a string of numbers parsed incorrectly.",
        102400l, builder.build().getMobThreshold());

    // ambiguous long that ends up a string
    builder.setValue(threshold, Bytes.toBytes(0x3030303030323030l));
    Assert.assertEquals("long that can parse as a string of numbers parsed incorrectly.",
        200l, builder.build().getMobThreshold());
    builder.setValue(threshold, Bytes.toBytes(0x2D30303030303031l));
    Assert.assertEquals("long that can parse as a string of numbers parsed incorrectly.",
        -1l, builder.build().getMobThreshold());

    // incorrect string that ends up a long
    builder.setValue(threshold, Bytes.toBytes("DEADBEEF"));
    Assert.assertEquals("string that is not a number parsed incorrectly.",
        0x4445414442454546l, builder.build().getMobThreshold());

    // incorrect strings that look plausible that end up a long
    builder.setValue(threshold, Bytes.toBytes("0x100000"));
    Assert.assertEquals("string that is hex instead of decimal parsed incorrectly.",
        0x3078313030303030l, builder.build().getMobThreshold());

    builder.setValue(threshold, Bytes.toBytes("100,000l"));
    Assert.assertEquals("string with commas and text parsed incorrectly.",
        0x3130302C3030306Cl, builder.build().getMobThreshold());

    builder.setValue(threshold, Bytes.toBytes("100,000 "));
    Assert.assertEquals("string with comma and space parsed incorrectly.",
        0x3130302C30303020l, builder.build().getMobThreshold());

    builder.setValue(threshold, Bytes.toBytes(" 100000 "));
    Assert.assertEquals("string with buffer whitespace parsed incorrectly.",
        0x2031303030303020l, builder.build().getMobThreshold());

    builder.setValue(threshold, Bytes.toBytes("1,000.00"));
    Assert.assertEquals("string with comma parsed incorrectly.",
        0x312C3030302E3030l, builder.build().getMobThreshold());

    builder.setValue(threshold, Bytes.toBytes("10000.00"));
    Assert.assertEquals("string with decimal place parsed incorrectly.",
        0x31303030302E3030l, builder.build().getMobThreshold());
  }

  @Test(expected=NumberFormatException.class)
  public void testMobThresholdStillRejectsNonNumbers() {
    ColumnFamilyDescriptorBuilder builder
        = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("mob"));
    byte[] threshold = Bytes.toBytes(ColumnFamilyDescriptorBuilder.MOB_THRESHOLD);

    builder.setValue(threshold, Bytes.toBytes("some string that is not eight bytes"));
    builder.build().getMobThreshold();

  }

  @Test(expected=NumberFormatException.class)
  public void testMobthresholdStillRejectsMixed() {
    ColumnFamilyDescriptorBuilder builder
        = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("mob"));
    byte[] threshold = Bytes.toBytes(ColumnFamilyDescriptorBuilder.MOB_THRESHOLD);

    builder.setValue(threshold, Bytes.toBytes("1234five!"));
    builder.build().getMobThreshold();

  }

  @Test(expected=NumberFormatException.class)
  public void testMobthresholdStillRejectsCommasMostly() {
    ColumnFamilyDescriptorBuilder builder
        = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("mob"));
    byte[] threshold = Bytes.toBytes(ColumnFamilyDescriptorBuilder.MOB_THRESHOLD);

    builder.setValue(threshold, Bytes.toBytes("1,000,000"));
    builder.build().getMobThreshold();

  }
}
