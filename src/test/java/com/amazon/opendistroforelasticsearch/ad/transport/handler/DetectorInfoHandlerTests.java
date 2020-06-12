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

package com.amazon.opendistroforelasticsearch.ad.transport.handler;

import static org.mockito.Mockito.mock;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInfo;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingConsumerWrapper;

public class DetectorInfoHandlerTests extends ESTestCase {
    private DetectorInfoHandler detectorInfoHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AnomalyDetectionIndices anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        Client client = mock(Client.class);
        Settings settings = Settings.EMPTY;
        ClientUtil clientUtil = mock(ClientUtil.class);
        IndexUtils indexUtils = mock(IndexUtils.class);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        detectorInfoHandler = new DetectorInfoHandler(
            client,
            settings,
            threadPool,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initDetectorInfoIndex),
            anomalyDetectionIndices::doesDetectorInfoIndexExist,
            clientUtil,
            indexUtils,
            clusterService
        );
    }

    public void testRcfUpdates() {
        long updates = 10;
        DetectorInfoHandler.TotalRcfUpdatesStrategy rcfStrategy = detectorInfoHandler.new TotalRcfUpdatesStrategy(updates);
        DetectorInfo info = rcfStrategy.createNewInfo(null);
        assertTrue(null == info.getError());
        assertEquals(updates, info.getRcfUpdates());
        assertTrue(info.getLastUpdateTime() != null);
    }
}
