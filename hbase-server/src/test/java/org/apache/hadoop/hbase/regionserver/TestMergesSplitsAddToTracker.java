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
package org.apache.hadoop.hbase.regionserver;

  import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker.STORE_FILE_TRACKER;
  import static org.junit.Assert.assertTrue;

  import java.io.IOException;
  import java.util.ArrayList;
  import java.util.Collection;
  import java.util.List;

  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.fs.FileStatus;
  import org.apache.hadoop.fs.FileSystem;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.hbase.HBaseClassTestRule;
  import org.apache.hadoop.hbase.HBaseTestingUtil;
  import org.apache.hadoop.hbase.TableName;
  import org.apache.hadoop.hbase.client.Put;
  import org.apache.hadoop.hbase.client.RegionInfo;
  import org.apache.hadoop.hbase.client.RegionInfoBuilder;
  import org.apache.hadoop.hbase.client.Table;
  import org.apache.hadoop.hbase.regionserver.storefiletracker.DummyStoreFileTracker;
  import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
  import org.apache.hadoop.hbase.testclassification.LargeTests;
  import org.apache.hadoop.hbase.testclassification.RegionServerTests;
  import org.apache.hadoop.hbase.util.Bytes;
  import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
  import org.junit.AfterClass;
  import org.junit.BeforeClass;
  import org.junit.ClassRule;
  import org.junit.Rule;
  import org.junit.Test;
  import org.junit.experimental.categories.Category;
  import org.junit.rules.TestName;


@Category({RegionServerTests.class, LargeTests.class})
public class TestMergesSplitsAddToTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMergesSplitsAddToTracker.class);

  private static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  public static final byte[] FAMILY_NAME = Bytes.toBytes("info");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.getConfiguration().set(STORE_FILE_TRACKER, DummyStoreFileTracker.class.getName());
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCommitDaughterRegion() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(table, FAMILY_NAME);
    //first put some data in order to have a store file created
    putThreeRowsAndFlush(table);
    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(table).get(0);
    HRegionFileSystem regionFS = region.getStores().get(0).getRegionFileSystem();
    RegionInfo daughterA =
      RegionInfoBuilder.newBuilder(table).setStartKey(region.getRegionInfo().getStartKey()).
        setEndKey(Bytes.toBytes("002")).setSplit(false).
        setRegionId(region.getRegionInfo().getRegionId() +
          EnvironmentEdgeManager.currentTime()).build();
    RegionInfo daughterB = RegionInfoBuilder.newBuilder(table).setStartKey(Bytes.toBytes("002"))
      .setEndKey(region.getRegionInfo().getEndKey()).setSplit(false)
      .setRegionId(region.getRegionInfo().getRegionId()).build();
    HStoreFile file = (HStoreFile) region.getStore(FAMILY_NAME).getStorefiles().toArray()[0];
    List<Path> splitFilesA = new ArrayList<>();
    splitFilesA.add(regionFS
      .splitStoreFile(daughterA, Bytes.toString(FAMILY_NAME), file,
        Bytes.toBytes("002"), false, region.getSplitPolicy()));
    List<Path> splitFilesB = new ArrayList<>();
    splitFilesB.add(regionFS
      .splitStoreFile(daughterB, Bytes.toString(FAMILY_NAME), file,
        Bytes.toBytes("002"), true, region.getSplitPolicy()));
    Path resultA = regionFS.commitDaughterRegion(daughterA, splitFilesA);
    Path resultB = regionFS.commitDaughterRegion(daughterB, splitFilesB);
    FileSystem fs = regionFS.getFileSystem();
    verifyFilesAreTracked(resultA, fs);
    verifyFilesAreTracked(resultB, fs);
  }

  @Test
  public void testCommitMergedRegion() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(table, FAMILY_NAME);
    //splitting the table first
    TEST_UTIL.getAdmin().split(table, Bytes.toBytes("002"));
    //Add data and flush to create files in the two different regions
    putThreeRowsAndFlush(table);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(table);
    HRegion first = regions.get(0);
    HRegion second = regions.get(1);
    HRegionFileSystem regionFS = first.getRegionFileSystem();

    RegionInfo mergeResult =
      RegionInfoBuilder.newBuilder(table).setStartKey(first.getRegionInfo().getStartKey())
        .setEndKey(second.getRegionInfo().getEndKey()).setSplit(false)
        .setRegionId(first.getRegionInfo().getRegionId() +
          EnvironmentEdgeManager.currentTime()).build();

    HRegionFileSystem mergeFS = HRegionFileSystem.createRegionOnFileSystem(
      TEST_UTIL.getHBaseCluster().getMaster().getConfiguration(),
      regionFS.getFileSystem(), regionFS.getTableDir(), mergeResult);

    Path mergeDir = regionFS.getMergesDir(mergeResult);

    List<Path> mergedFiles = new ArrayList<>();
    //merge file from first region
    mergedFiles.add(mergeFileFromRegion(first, mergeFS));
    //merge file from second region
    mergedFiles.add(mergeFileFromRegion(second, mergeFS));
    first.getRegionFileSystem().commitMergedRegion(mergedFiles);
    //validate
    FileSystem fs = first.getRegionFileSystem().getFileSystem();
    Path finalMergeDir = new Path(first.getRegionFileSystem().getTableDir(),
      mergeResult.getEncodedName());
    verifyFilesAreTracked(finalMergeDir, fs);
  }

  private void verifyFilesAreTracked(Path regionDir, FileSystem fs) throws Exception {
    for(FileStatus f : fs.listStatus(new Path(regionDir, Bytes.toString(FAMILY_NAME)))){
      assertTrue(DummyStoreFileTracker.trackedFiles.contains(f.getPath()));
    }
  }

  private Path mergeFileFromRegion(HRegion regionToMerge, HRegionFileSystem mergeFS)
      throws IOException {
    HStoreFile file = (HStoreFile) regionToMerge.getStore(FAMILY_NAME).getStorefiles().toArray()[0];
    return mergeFS.mergeStoreFile(regionToMerge.getRegionInfo(), Bytes.toString(FAMILY_NAME), file);
  }

  private void putThreeRowsAndFlush(TableName table) throws IOException {
    Table tbl = TEST_UTIL.getConnection().getTable(table);
    Put put = new Put(Bytes.toBytes("001"));
    byte[] qualifier = Bytes.toBytes("1");
    put.addColumn(FAMILY_NAME, qualifier, Bytes.toBytes(1));
    tbl.put(put);
    put = new Put(Bytes.toBytes("002"));
    put.addColumn(FAMILY_NAME, qualifier, Bytes.toBytes(2));
    tbl.put(put);
    put = new Put(Bytes.toBytes("003"));
    put.addColumn(FAMILY_NAME, qualifier, Bytes.toBytes(2));
    tbl.put(put);
    TEST_UTIL.flush(table);
  }
}
