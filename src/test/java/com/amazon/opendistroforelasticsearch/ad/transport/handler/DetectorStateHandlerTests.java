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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Instant;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingConsumerWrapper;

public class DetectorStateHandlerTests extends ESTestCase {
    private DetectorStateHandler detectorStateHandler;
    private String detectorId = "123";
    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        AnomalyDetectionIndices anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        client = mock(Client.class);
        Settings settings = Settings.EMPTY;
        Clock clock = mock(Clock.class);
        Throttler throttler = new Throttler(clock);
        ThreadPool threadpool = mock(ThreadPool.class);
        ClientUtil clientUtil = new ClientUtil(Settings.EMPTY, client, throttler, threadpool);
        IndexUtils indexUtils = mock(IndexUtils.class);
        ClusterService clusterService = mock(ClusterService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        detectorStateHandler = new DetectorStateHandler(
            client,
            settings,
            threadPool,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initDetectorStateIndex),
            anomalyDetectionIndices::doesDetectorStateIndexExist,
            clientUtil,
            indexUtils,
            clusterService
        );
    }

    public void testNullState() {
        long updates = 10;
        DetectorStateHandler.TotalRcfUpdatesStrategy rcfStrategy = detectorStateHandler.new TotalRcfUpdatesStrategy(updates);
        DetectorInternalState state = rcfStrategy.createNewState(null);
        assertTrue(null == state.getError());
        assertEquals(updates, state.getRcfUpdates());
        assertTrue(state.getLastUpdateTime() != null);
    }

    public void testNonNullState() {
        long updates = 10;
        String error = "blah";
        DetectorInternalState oldState = new DetectorInternalState.Builder().error(error).lastUpdateTime(Instant.ofEpochSecond(1L)).build();
        DetectorStateHandler.TotalRcfUpdatesStrategy rcfStrategy = detectorStateHandler.new TotalRcfUpdatesStrategy(updates);
        DetectorInternalState state = rcfStrategy.createNewState(oldState);
        assertTrue(error.equals(state.getError()));
        assertEquals(updates, state.getRcfUpdates());
        assertTrue(state.getLastUpdateTime().getEpochSecond() > 1L);
    }

    public void testNoUpdateWith0RCFUpdate() {
        detectorStateHandler.saveRcfUpdates(0, detectorId);

        verify(client, never()).get(any(), any());
    }

    public void testNoUpdateWithNonZeroRCFUpdate() {
        detectorStateHandler.saveRcfUpdates(1, detectorId);

        verify(client, times(1)).get(any(), any());
    }
}
