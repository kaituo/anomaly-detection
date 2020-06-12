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

import com.amazon.opendistroforelasticsearch.ad.model.DetectorInfo;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;

public class DetectorInfoHandler extends AnomalyIndexHandler<DetectorInfo> {
    interface GetInfoStrategy {
        DetectorInfo createNewInfo(DetectorInfo info);
    }

    class TotalRcfUpdatesStrategy implements GetInfoStrategy {
        private long totalRcfUpdates;

        TotalRcfUpdatesStrategy(long totalRcfUpdates) {
            this.totalRcfUpdates = totalRcfUpdates;
        }

        @Override
        public DetectorInfo createNewInfo(DetectorInfo info) {
            DetectorInfo newInfo = null;
            if (info == null) {
                newInfo = new DetectorInfo.Builder().rcfUpdates(totalRcfUpdates).lastUpdateTime(Instant.now()).build();
            } else {
                newInfo = (DetectorInfo) info.clone();
                newInfo.setRcfUpdates(totalRcfUpdates);
                newInfo.setLastUpdateTime(Instant.now());
            }
            return newInfo;
        }
    }

    class ErrorStrategy implements GetInfoStrategy {
        private String error;

        ErrorStrategy(String error) {
            this.error = error;
        }

        @Override
        public DetectorInfo createNewInfo(DetectorInfo info) {
            DetectorInfo newInfo = null;
            if (info == null) {
                newInfo = new DetectorInfo.Builder().error(error).lastUpdateTime(Instant.now()).build();
            } else {
                newInfo = (DetectorInfo) info.clone();
                newInfo.setError(error);
                newInfo.setLastUpdateTime(Instant.now());
            }

            return newInfo;
        }
    }

    private static final Logger LOG = LogManager.getLogger(DetectorInfoHandler.class);

    public DetectorInfoHandler(
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
            DetectorInfo.ANOMALY_INFO_INDEX,
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
            LOG.info(String.format("Don't save the info of detector %s as its total updates is 0", detectorId));
            return;
        }

        update(detectorId, new TotalRcfUpdatesStrategy(totalRcfUpdates));
    }

    public void saveError(String error, String detectorId) {
        update(detectorId, new ErrorStrategy(error));
    }

    /**
     * Updates a detector's info according to GetInfoHandler
     * @param detectorId detector id
     * @param handler specify how to convert from existing info object to an object we want to save
     */
    private void update(String detectorId, GetInfoStrategy handler) {
        try {
            GetRequest getRequest = new GetRequest(this.indexName).id(detectorId);

            clientUtil.<GetRequest, GetResponse>asyncRequest(getRequest, client::get, ActionListener.wrap(response -> {
                DetectorInfo newInfo = null;
                if (response.isExists()) {
                    try (
                        XContentParser parser = XContentType.JSON
                            .xContent()
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
                    ) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                        DetectorInfo info = DetectorInfo.parse(parser);
                        newInfo = handler.createNewInfo(info);

                    } catch (IOException e) {
                        LOG.error("Failed to update AD info for " + detectorId, e);
                        return;
                    }
                } else {
                    newInfo = handler.createNewInfo(null);
                }
                super.index(newInfo, detectorId);
            }, exception -> {
                Throwable cause = ExceptionsHelper.unwrapCause(exception);
                if (cause instanceof IndexNotFoundException) {
                    super.index(handler.createNewInfo(null), detectorId);
                } else {
                    LOG.error("Failed to get detector info " + detectorId, exception);
                }
            }));
        } catch (Exception e) {
            LOG.error("Failed to update AD info for " + detectorId, e);
        }
    }
}
