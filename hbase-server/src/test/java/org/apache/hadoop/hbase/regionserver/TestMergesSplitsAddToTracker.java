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

import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker.
  STORE_FILE_TRACKER;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.regionserver.storefiletracker.TestStoreFileTracker;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Before;
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
  public static void setupClass() throws Exception {
    TEST_UTIL.getConfiguration().set(STORE_FILE_TRACKER, TestStoreFileTracker.class.getName());
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup(){
    TestStoreFileTracker.trackedFiles = new HashMap<>();
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
          EnvironmentEdgeManager.currentTime()).
        build();
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
    MasterProcedureEnv env = TEST_UTIL.getMiniHBaseCluster().getMaster().
      getMasterProcedureExecutor().getEnvironment();
    Path resultA = regionFS.commitDaughterRegion(daughterA, splitFilesA, env);
    Path resultB = regionFS.commitDaughterRegion(daughterB, splitFilesB, env);
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

    List<Path> mergedFiles = new ArrayList<>();
    //merge file from first region
    mergedFiles.add(mergeFileFromRegion(first, mergeFS));
    //merge file from second region
    mergedFiles.add(mergeFileFromRegion(second, mergeFS));
    MasterProcedureEnv env = TEST_UTIL.getMiniHBaseCluster().getMaster().
      getMasterProcedureExecutor().getEnvironment();
    mergeFS.commitMergedRegion(mergedFiles, env);
    //validate
    FileSystem fs = first.getRegionFileSystem().getFileSystem();
    Path finalMergeDir = new Path(first.getRegionFileSystem().getTableDir(),
      mergeResult.getEncodedName());
    verifyFilesAreTracked(finalMergeDir, fs);
  }

  @Test
  public void testSplitLoadsFromTracker() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(table, FAMILY_NAME);
    //Add data and flush to create files in the two different regions
    putThreeRowsAndFlush(table);
    HRegion region = TEST_UTIL.getHBaseCluster().getRegions(table).get(0);
    Pair<StoreFileInfo, String> copyResult = copyFileInTheStoreDir(region);
    StoreFileInfo fileInfo = copyResult.getFirst();
    String copyName = copyResult.getSecond();
    //Now splits the region
    TEST_UTIL.getAdmin().split(table, Bytes.toBytes("002"));
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(table);
    HRegion first = regions.get(0);
    validateDaughterRegionsFiles(first, fileInfo.getActiveFileName(), copyName);
    HRegion second = regions.get(1);
    validateDaughterRegionsFiles(second, fileInfo.getActiveFileName(), copyName);
  }

  @Test
  public void testMergeLoadsFromTracker() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(table, new byte[][]{FAMILY_NAME},
      new byte[][]{Bytes.toBytes("002")});
    //Add data and flush to create files in the two different regions
    putThreeRowsAndFlush(table);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(table);
    HRegion first = regions.get(0);
    Pair<StoreFileInfo, String> copyResult = copyFileInTheStoreDir(first);
    StoreFileInfo fileInfo = copyResult.getFirst();
    String copyName = copyResult.getSecond();
    //Now merges the first two regions
    TEST_UTIL.getAdmin().mergeRegionsAsync(new byte[][]{
      first.getRegionInfo().getEncodedNameAsBytes(),
      regions.get(1).getRegionInfo().getEncodedNameAsBytes()
    }, true).get(10, TimeUnit.SECONDS);
    regions = TEST_UTIL.getHBaseCluster().getRegions(table);
    HRegion merged = regions.get(0);
    validateDaughterRegionsFiles(merged, fileInfo.getActiveFileName(), copyName);
  }

  private Pair<StoreFileInfo,String> copyFileInTheStoreDir(HRegion region) throws IOException {
    Path storeDir = region.getRegionFileSystem().getStoreDir("info");
    //gets the single file
    StoreFileInfo fileInfo = region.getRegionFileSystem().getStoreFiles("info").get(0);
    //make a copy of the valid file staight into the store dir, so that it's not tracked.
    String copyName = UUID.randomUUID().toString().replaceAll("-", "");
    Path copy = new Path(storeDir, copyName);
    FileUtil.copy(region.getFilesystem(), fileInfo.getFileStatus(), region.getFilesystem(),
      copy , false, false, TEST_UTIL.getConfiguration());
    return new Pair<>(fileInfo, copyName);
  }

  private void validateDaughterRegionsFiles(HRegion region, String orignalFileName,
      String untrackedFile) throws IOException {
    //verify there's no link for the untracked, copied file in first region
    List<StoreFileInfo> infos = region.getRegionFileSystem().getStoreFiles("info");
    final MutableBoolean foundLink = new MutableBoolean(false);
    infos.stream().forEach(i -> {
      i.getActiveFileName().contains(orignalFileName);
      if(i.getActiveFileName().contains(untrackedFile)){
        fail();
      }
      if(i.getActiveFileName().contains(orignalFileName)){
        foundLink.setTrue();
      }
    });
    assertTrue(foundLink.booleanValue());
  }

  private void verifyFilesAreTracked(Path regionDir, FileSystem fs) throws Exception {
    String storeId = regionDir.getName() + "-info";
    for(FileStatus f : fs.listStatus(new Path(regionDir, Bytes.toString(FAMILY_NAME)))){
      assertTrue(TestStoreFileTracker.trackedFiles.get(storeId).stream().filter( s ->
        s.getPath().equals(f.getPath())).findFirst().isPresent());
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
