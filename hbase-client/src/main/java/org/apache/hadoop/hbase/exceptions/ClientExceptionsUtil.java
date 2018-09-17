/*
 *
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

package org.apache.hadoop.hbase.exceptions;

import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.RetryImmediatelyException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.ipc.RemoteException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ClientExceptionsUtil {

  private ClientExceptionsUtil() {}

  public static boolean isMetaClearingException(Throwable cur) {
    cur = findException(cur);

    if (cur == null) {
      return true;
    }
    return !isSpecialException(cur) || (cur instanceof RegionMovedException);
  }

  public static boolean isSpecialException(Throwable cur) {
    return (cur instanceof RegionMovedException || cur instanceof RegionOpeningException
        || cur instanceof RegionTooBusyException
        || cur instanceof ThrottlingException || cur instanceof RpcThrottlingException
        || cur instanceof MultiActionResultTooLarge || cur instanceof RetryImmediatelyException
        || cur instanceof CallQueueTooBigException || cur instanceof NotServingRegionException);
  }


  /**
   * Look for an exception we know in the remote exception:
   * - hadoop.ipc wrapped exceptions
   * - nested exceptions
   *
   * Looks for: RegionMovedException / RegionOpeningException / RegionTooBusyException /
   *            ThrottlingException
   * @return null if we didn't find the exception, the exception otherwise.
   */
  public static Throwable findException(Object exception) {
    if (exception == null || !(exception instanceof Throwable)) {
      return null;
    }
    Throwable cur = (Throwable) exception;
    while (cur != null) {
      if (isSpecialException(cur)) {
        return cur;
      }
      if (cur instanceof RemoteException) {
        RemoteException re = (RemoteException) cur;
        cur = re.unwrapRemoteException();

        // unwrapRemoteException can return the exception given as a parameter when it cannot
        //  unwrap it. In this case, there is no need to look further
        // noinspection ObjectEquality
        if (cur == re) {
          return cur;
        }

        // When we receive RemoteException which wraps IOException which has a cause as
        // RemoteException we can get into infinite loop here; so if the cause of the exception
        // is RemoteException, we shouldn't look further.
      } else if (cur.getCause() != null && !(cur.getCause() instanceof RemoteException)) {
        cur = cur.getCause();
      } else {
        return cur;
      }
    }

    return null;
  }
}
