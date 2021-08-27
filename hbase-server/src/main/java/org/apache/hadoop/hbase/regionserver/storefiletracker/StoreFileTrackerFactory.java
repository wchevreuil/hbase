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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker.
  STORE_FILE_TRACKER;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory method for creating store file tracker.
 */
@InterfaceAudience.Private
public final class StoreFileTrackerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileTrackerFactory.class);

  public static StoreFileTracker create(Configuration conf, TableName tableName,
      boolean isPrimaryReplica, StoreContext ctx) {
    String className = conf.get(STORE_FILE_TRACKER, DefaultStoreFileTracker.class.getName());
    try {
      LOG.info("instantiating StoreFileTracker impl {}", className);
      return ReflectionUtils.newInstance(
        (Class<? extends StoreFileTracker>) Class.forName(className), conf, tableName,
        isPrimaryReplica, ctx);
    } catch (Exception e) {
      LOG.error("Unable to create StoreFileTracker impl : {}", className, e);
      throw new RuntimeException(e);
    }
  }

  public static StoreFileTracker create(Configuration conf, TableName tableName,
    boolean isPrimaryReplica, String family, HRegionFileSystem regionFs) {
    ColumnFamilyDescriptorBuilder fDescBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));
    StoreContext ctx = StoreContext.getBuilder().
      withColumnFamilyDescriptor(fDescBuilder.build()).
      withRegionFileSystem(regionFs).
      build();
    return StoreFileTrackerFactory.create(conf, tableName, isPrimaryReplica, ctx);
  }

  public static Configuration mergeConfigurations(Configuration global,
    TableDescriptor table, ColumnFamilyDescriptor family) {
    if(!StringUtils.isEmpty(family.getConfigurationValue(STORE_FILE_TRACKER))){
      global.set(STORE_FILE_TRACKER, family.getConfigurationValue(STORE_FILE_TRACKER));
    } else if(!StringUtils.isEmpty(table.getValue(STORE_FILE_TRACKER))) {
      global.set(STORE_FILE_TRACKER, table.getValue(STORE_FILE_TRACKER));
    }
    return global;
  }
}
