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

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;

public class DetectorStateHandler extends AnomalyIndexHandler<DetectorInternalState> {
    interface GetStateStrategy {
        DetectorInternalState createNewState(DetectorInternalState state);
    }

    class TotalRcfUpdatesStrategy implements GetStateStrategy {
        private long totalRcfUpdates;

        TotalRcfUpdatesStrategy(long totalRcfUpdates) {
            this.totalRcfUpdates = totalRcfUpdates;
        }

        @Override
        public DetectorInternalState createNewState(DetectorInternalState state) {
            DetectorInternalState newState = null;
            if (state == null) {
                newState = new DetectorInternalState.Builder().rcfUpdates(totalRcfUpdates).lastUpdateTime(Instant.now()).build();
            } else {
                newState = (DetectorInternalState) state.clone();
                newState.setRcfUpdates(totalRcfUpdates);
                newState.setLastUpdateTime(Instant.now());
            }
            return newState;
        }
    }

    class ErrorStrategy implements GetStateStrategy {
        private String error;

        ErrorStrategy(String error) {
            this.error = error;
        }

        @Override
        public DetectorInternalState createNewState(DetectorInternalState state) {
            DetectorInternalState newState = null;
            if (state == null) {
                newState = new DetectorInternalState.Builder().error(error).lastUpdateTime(Instant.now()).build();
            } else {
                newState = (DetectorInternalState) state.clone();
                newState.setError(error);
                newState.setLastUpdateTime(Instant.now());
            }

            return newState;
        }
    }

    private static final Logger LOG = LogManager.getLogger(DetectorStateHandler.class);

    public DetectorStateHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        Consumer<ActionListener<CreateIndexResponse>> createIndex,
        BooleanSupplier indexExists,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService
    ) {
        super(
            client,
            settings,
            threadPool,
            DetectorInternalState.DETECTOR_STATE_INDEX,
            createIndex,
            indexExists,
            true,
            clientUtil,
            indexUtils,
            clusterService
        );
    }

    public void saveRcfUpdates(long totalRcfUpdates, String detectorId) {
        if (totalRcfUpdates == 0L) {
            // either initialization haven't started or all rcf partitions are missing
            LOG.info(String.format("Don't save the state of detector %s as its total updates is 0", detectorId));
            return;
        }

        update(detectorId, new TotalRcfUpdatesStrategy(totalRcfUpdates));
    }

    public void saveError(String error, String detectorId) {
        update(detectorId, new ErrorStrategy(error));
    }

    /**
     * Updates a detector's state according to GetStateHandler
     * @param detectorId detector id
     * @param handler specify how to convert from existing state object to an object we want to save
     */
    private void update(String detectorId, GetStateStrategy handler) {
        try {
            GetRequest getRequest = new GetRequest(this.indexName).id(detectorId);

            clientUtil.<GetRequest, GetResponse>asyncRequest(getRequest, client::get, ActionListener.wrap(response -> {
                DetectorInternalState newState = null;
                if (response.isExists()) {
                    try (
                        XContentParser parser = XContentType.JSON
                            .xContent()
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
                    ) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                        DetectorInternalState state = DetectorInternalState.parse(parser);
                        newState = handler.createNewState(state);

                    } catch (IOException e) {
                        LOG.error("Failed to update AD state for " + detectorId, e);
                        return;
                    }
                } else {
                    newState = handler.createNewState(null);
                }
                super.index(newState, detectorId);
            }, exception -> {
                Throwable cause = ExceptionsHelper.unwrapCause(exception);
                if (cause instanceof IndexNotFoundException) {
                    super.index(handler.createNewState(null), detectorId);
                } else {
                    LOG.error("Failed to get detector state " + detectorId, exception);
                }
            }));
        } catch (Exception e) {
            LOG.error("Failed to update AD state for " + detectorId, e);
        }
    }
}
