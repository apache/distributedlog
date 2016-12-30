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
package com.twitter.distributedlog.service.placement;

import com.twitter.util.Future;

/**
 * Created for those who hold these truths to be self-evident, that all streams are created equal,
 * that they are endowed by their creator with certain unalienable loads, that among these are
 * Uno, Eins, and One.
 */
public class EqualLoadAppraiser implements LoadAppraiser {
  @Override
  public Future<StreamLoad> getStreamLoad(String stream) {
    return Future.value(new StreamLoad(stream, 1));
  }

  @Override
  public Future<Void> refreshCache() {
    return Future.value(null);
  }
}
