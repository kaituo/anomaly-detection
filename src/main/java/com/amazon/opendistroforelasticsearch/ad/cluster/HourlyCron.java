/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;

import com.amazon.opendistroforelasticsearch.ad.transport.CronAction;
import com.amazon.opendistroforelasticsearch.ad.transport.CronRequest;
import com.amazon.opendistroforelasticsearch.ad.util.ClusterStateUtils;

public class HourlyCron implements Runnable {
    private static final Logger LOG = LogManager.getLogger(HourlyCron.class);
    static final String SUCCEEDS_LOG_MSG = "Hourly maintenance succeeds";
    static final String NODE_EXCEPTION_LOG_MSG = "Hourly maintenance of node has exception";
    static final String EXCEPTION_LOG_MSG = "Hourly maintenance has exception.";
    private ClusterStateUtils clientStateUtils;
    private Client client;

    public HourlyCron(Client client, ClusterStateUtils clientStateUtils) {
        this.clientStateUtils = clientStateUtils;
        this.client = client;
    }

    @Override
    public void run() {
        DiscoveryNode[] dataNodes = clientStateUtils.getEligibleDataNodes().values().toArray(DiscoveryNode.class);

        // we also add the cancel query function here based on query text from the negative cache.

        CronRequest modelDeleteRequest = new CronRequest(dataNodes);
        client.execute(CronAction.INSTANCE, modelDeleteRequest, ActionListener.wrap(response -> {
            if (response.hasFailures()) {
                for (FailedNodeException failedNodeException : response.failures()) {
                    LOG.warn(NODE_EXCEPTION_LOG_MSG, failedNodeException);
                }
            } else {
                LOG.info(SUCCEEDS_LOG_MSG);
            }
        }, exception -> { LOG.error(EXCEPTION_LOG_MSG, exception); }));
    }
}
