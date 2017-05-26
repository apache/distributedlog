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
package org.apache.distributedlog.service.stream.admin;

import static org.apache.distributedlog.service.stream.AbstractStreamOp.requestStat;

import org.apache.distributedlog.service.ResponseUtils;
import org.apache.distributedlog.service.stream.StreamManager;
import org.apache.distributedlog.thrift.service.WriteResponse;
import com.twitter.util.Future;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.runtime.AbstractFunction1;

/**
 * Operation to create log stream.
 */
public class CreateOp extends StreamAdminOp {

  public CreateOp(String stream,
                  StatsLogger statsLogger,
                  StreamManager streamManager,
                  Long checksum,
                  Feature checksumEnabledFeature) {
    super(stream,
            streamManager,
            requestStat(statsLogger, "create"),
            checksum,
            checksumEnabledFeature);
  }

  @Override
  protected Future<WriteResponse> executeOp() {
    Future<Void> result = streamManager.createStreamAsync(stream);
    return result.map(new AbstractFunction1<Void, WriteResponse>() {
      @Override
      public WriteResponse apply(Void value) {
        return ResponseUtils.writeSuccess();
      }
    });
  }
}
