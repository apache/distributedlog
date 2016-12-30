/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.service.placement;

import java.util.TreeSet;

/**
 * The PlacementStateManager handles persistence of calculated resource placements.
 */
public interface PlacementStateManager {

    /**
     * Saves the ownership mapping as a TreeSet of ServerLoads to persistent storage.
     */
    void saveOwnership(TreeSet<ServerLoad> serverLoads) throws StateManagerSaveException;

    /**
     * Loads the ownership mapping as TreeSet of ServerLoads from persistent storage.
     */
    TreeSet<ServerLoad> loadOwnership() throws StateManagerLoadException;

    /**
     * Watch the persistent storage for changes to the ownership mapping.
     *
     * <p>The placementCallback callbacks will be triggered with the new mapping when a change occurs.
     */
    void watch(PlacementCallback placementCallback);

    /**
     * Placement Callback.
     *
     * <p>The callback is triggered when server loads are updated.
     */
    interface PlacementCallback {
        void callback(TreeSet<ServerLoad> serverLoads);
    }

    /**
     * The base exception thrown when state manager encounters errors.
     */
    abstract class StateManagerException extends Exception {
        public StateManagerException(String message, Exception e) {
            super(message, e);
        }
    }

    /**
     * Exception thrown when failed to load the ownership mapping.
     */
    class StateManagerLoadException extends StateManagerException {
        public StateManagerLoadException(Exception e) {
            super("Load of Ownership failed", e);
        }
    }

    /**
     * Exception thrown when failed to save the ownership mapping.
     */
    class StateManagerSaveException extends StateManagerException {
        public StateManagerSaveException(Exception e) {
            super("Save of Ownership failed", e);
        }
    }
}
