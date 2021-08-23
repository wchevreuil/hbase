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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DummyStoreFileTracker  extends DefaultStoreFileTracker {

  public static List<Path> trackedFiles = new ArrayList<>();

  public DummyStoreFileTracker(Configuration conf, TableName tableName, Boolean isPrimaryReplica,
    StoreContext ctx) {
    super(conf, tableName, isPrimaryReplica, ctx);
  }

  @Override
  protected void doAddNewStoreFiles(Collection<StoreFileInfo> newFiles) throws IOException {
    newFiles.stream().forEach( s -> trackedFiles.add(s.getPath()));
  }

}
