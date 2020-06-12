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

package com.amazon.opendistroforelasticsearch.ad;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfile;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorState;
import com.amazon.opendistroforelasticsearch.ad.model.InitProgressProfile;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.model.ProfileName;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingResponse;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;

public class AnomalyDetectorProfileRunner {
    private final Logger logger = LogManager.getLogger(AnomalyDetectorProfileRunner.class);
    private Client client;
    private NamedXContentRegistry xContentRegistry;
    private DiscoveryNodeFilterer nodeFilter;
    static String FAIL_TO_FIND_DETECTOR_MSG = "Fail to find detector with id: ";
    static String FAIL_TO_GET_PROFILE_MSG = "Fail to get profile for detector ";
    private long requiredSamples;

    public AnomalyDetectorProfileRunner(
        Client client,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        long requiredSamples
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        this.requiredSamples = requiredSamples;
    }

    public void profile(String detectorId, ActionListener<DetectorProfile> listener, Set<ProfileName> profilesToCollect) {

        if (profilesToCollect.isEmpty()) {
            listener.onFailure(new RuntimeException("Unsupported profile types."));
            return;
        }

        // total number of listeners we need to define. Needed by MultiResponsesDelegateActionListener to decide when to consolidate results
        // and return to users
        int totalListener = 0;

        if (profilesToCollect.contains(ProfileName.STATE)) {
            totalListener++;
        }

        if (profilesToCollect.contains(ProfileName.ERROR)) {
            totalListener++;
        }

        if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
            totalListener++;
        }

        if (profilesToCollect.contains(ProfileName.COORDINATING_NODE)
            || profilesToCollect.contains(ProfileName.SHINGLE_SIZE)
            || profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
            || profilesToCollect.contains(ProfileName.MODELS)) {
            totalListener++;
        }

        MultiResponsesDelegateActionListener<DetectorProfile> delegateListener = new MultiResponsesDelegateActionListener<DetectorProfile>(
            listener,
            totalListener,
            "Fail to fetch profile for " + detectorId
        );

        prepareProfile(detectorId, delegateListener, profilesToCollect);
    }

    private void prepareProfile(
        String detectorId,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        Set<ProfileName> profilesToCollect
    ) {
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX, detectorId);
        client.get(getRequest, ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                    long enabledTimeMs = job.getEnabledTime().toEpochMilli();

                    if (profilesToCollect.contains(ProfileName.ERROR)) {
                        GetRequest getErrorRequest = new GetRequest(DetectorInternalState.DETECTOR_STATE_INDEX, detectorId);
                        client.get(getErrorRequest, onGetError(listener, detectorId));
                    }

                    if (profilesToCollect.contains(ProfileName.STATE) || profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
                        profileStateRelated(detectorId, enabledTimeMs, listener, job.isEnabled(), profilesToCollect);
                    }

                    if (profilesToCollect.contains(ProfileName.COORDINATING_NODE)
                        || profilesToCollect.contains(ProfileName.SHINGLE_SIZE)
                        || profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
                        || profilesToCollect.contains(ProfileName.MODELS)) {
                        profileModels(detectorId, profilesToCollect, listener);
                    }
                } catch (IOException | XContentParseException | NullPointerException e) {
                    logger.error(e);
                    listener.failImmediately(FAIL_TO_GET_PROFILE_MSG, e);
                }
            } else {
                GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
                client.get(getDetectorRequest, onGetDetectorForPrepare(listener, detectorId, profilesToCollect));
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(exception.getMessage());
                GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
                client.get(getDetectorRequest, onGetDetectorForPrepare(listener, detectorId, profilesToCollect));
            } else {
                logger.error(FAIL_TO_GET_PROFILE_MSG + detectorId);
                listener.onFailure(exception);
            }
        }));
    }

    private ActionListener<GetResponse> onGetDetectorForPrepare(
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        String detectorId,
        Set<ProfileName> profiles
    ) {
        return ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                if (profiles.contains(ProfileName.STATE)) {
                    profileBuilder.state(DetectorState.DISABLED);
                }
                listener.respondImmediately(profileBuilder.build());
            } else {
                listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId);
            }
        }, exception -> { listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId, exception); });
    }

    /**
     * We expect three kinds of states:
     *  -Disabled: if get ad job api says the job is disabled;
     *  -Init: if rcf model's total updates is less than required
     *  -Running: if neither of the above applies and no exceptions.
     * @param detectorId detector id
     * @param enabledTime the time when AD job is enabled in milliseconds
     * @param listener listener to process the returned state or exception
     * @param enabled whether the detector job is enabled or not
     * @param profilesToCollect target profiles to fetch
     */
    private void profileStateRelated(
        String detectorId,
        long enabledTime,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        boolean enabled,
        Set<ProfileName> profilesToCollect
    ) {
        if (enabled) {
            RCFPollingRequest request = new RCFPollingRequest(detectorId);
            client.execute(RCFPollingAction.INSTANCE, request, onPollRCFUpdates(detectorId, profilesToCollect, listener, enabledTime));
        } else if (profilesToCollect.contains(ProfileName.STATE)) {
            listener.onResponse(new DetectorProfile.Builder().state(DetectorState.DISABLED).build());
        }
    }

    /**
     * Action listener for a detector in running or init state
     * @param listener listener to consolidate results and return a final response
     * @param detectorId detector id
     * @param enabledTimeMs AD job enabled time
     * @param profilesToCollect target profiles to fetch
     * @return the listener for a detector in running or init state
     */
    private ActionListener<GetResponse> onGetEnabledDetectorState(
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        String detectorId,
        long enabledTimeMs,
        Set<ProfileName> profilesToCollect
    ) {
        return ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    DetectorInternalState detectorState = DetectorInternalState.parse(parser);
                    long stateLastUpdateTimeMs = detectorState.getLastUpdateTime().toEpochMilli();
                    if (stateLastUpdateTimeMs < enabledTimeMs) {
                        // state index hasn't been updated yet
                        respondEmptyInitProfile(listener, profilesToCollect);
                    } else {
                        long totalUpdates = detectorState.getRcfUpdates();
                        if (totalUpdates < requiredSamples) {
                            if (profilesToCollect.contains(ProfileName.STATE)) {
                                listener.onResponse(new DetectorProfile.Builder().state(DetectorState.INIT).build());
                            }

                            if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
                                if (totalUpdates < 0) {
                                    // no totalUpdates record in the detector state index
                                    listener.onResponse(getEmptyInitProfile(ProfileName.INIT_PROGRESS));
                                } else {
                                    GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
                                    client
                                        .get(
                                            getDetectorRequest,
                                            onGetDetectorForInitProgress(
                                                listener,
                                                detectorId,
                                                profilesToCollect,
                                                totalUpdates,
                                                requiredSamples
                                            )
                                        );
                                }
                            }

                        } else {
                            if (profilesToCollect.contains(ProfileName.STATE)) {
                                listener.onResponse(new DetectorProfile.Builder().state(DetectorState.RUNNING).build());
                            }
                            if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
                                InitProgressProfile initProgress = new InitProgressProfile("100%", 0, 0);
                                listener.onResponse(new DetectorProfile.Builder().initProgress(initProgress).build());
                            }
                        }

                    }
                } catch (IOException | XContentParseException | NullPointerException e) {
                    logger.error(e);
                    listener.failImmediately(FAIL_TO_GET_PROFILE_MSG, e);
                }
            } else {
                // detector state for this detector does not exist
                respondEmptyInitProfile(listener, profilesToCollect);
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                // detector state index is not created yet
                respondEmptyInitProfile(listener, profilesToCollect);
            } else {
                listener.failImmediately(String.format("Fail to find any detector state for detector {}", detectorId), exception);
            }
        });
    }

    /**
     * Action listener for getting error in internal state index
     * @param listener listener to consolidate results and return a final response
     * @param detectorId detector id
     * @return the listener for a detector in disabled state
     */
    private ActionListener<GetResponse> onGetError(MultiResponsesDelegateActionListener<DetectorProfile> listener, String detectorId) {
        return ActionListener.wrap(getResponse -> {
            DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    DetectorInternalState detectorState = DetectorInternalState.parse(parser);

                    if (detectorState.getError() != null) {
                        profileBuilder.error(detectorState.getError());
                    }

                    listener.onResponse(profileBuilder.build());

                } catch (IOException | XContentParseException | NullPointerException e) {
                    logger.error(e);
                    listener.failImmediately(FAIL_TO_GET_PROFILE_MSG, e);
                }
            } else {
                // detector state for this detector does not exist
                listener.onResponse(profileBuilder.build());
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                // detector state index is not created yet
                listener.onResponse(new DetectorProfile.Builder().build());
            } else {
                logger.error("Fail to find any detector state for detector {}", detectorId);
                listener.onFailure(exception);
            }
        });
    }

    private DetectorProfile getEmptyInitProfile(ProfileName profileToCollect) {
        DetectorProfile.Builder profile = new DetectorProfile.Builder();
        if (profileToCollect.equals(ProfileName.STATE)) {
            profile.state(DetectorState.INIT);
        }
        if (profileToCollect.equals(ProfileName.INIT_PROGRESS)) {
            InitProgressProfile initProgress = new InitProgressProfile("0%", 0, 0);
            profile.initProgress(initProgress);
        }
        return profile.build();
    }

    private void respondEmptyInitProfile(
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        Set<ProfileName> profilesToCollect
    ) {
        if (profilesToCollect.contains(ProfileName.STATE)) {
            listener.onResponse(getEmptyInitProfile(ProfileName.STATE));
        }
        if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
            listener.onResponse(getEmptyInitProfile(ProfileName.INIT_PROGRESS));
        }
    }

    private ActionListener<GetResponse> onGetDetectorForInitProgress(
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        String detectorId,
        Set<ProfileName> profilesToCollect,
        long totalUpdates,
        long requiredSamples
    ) {
        return ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                    AnomalyDetector detector = AnomalyDetector.parse(parser, detectorId);
                    long intervalMins = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMinutes();
                    float percent = (100.0f * totalUpdates) / requiredSamples;
                    int neededPoints = (int) (requiredSamples - totalUpdates);
                    InitProgressProfile initProgress = new InitProgressProfile(
                        // rounding: 93.456 => 93%, 93.556 => 94%
                        String.format("%.0f%%", percent),
                        intervalMins * neededPoints,
                        neededPoints
                    );

                    listener.onResponse(new DetectorProfile.Builder().initProgress(initProgress).build());
                } catch (Exception t) {
                    logger.error("Fail to parse detector {}", detectorId);
                    logger.error("Stack trace:", t);
                    listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId, t);
                }
            } else {
                listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId);
            }
        }, exception -> { listener.failImmediately(FAIL_TO_FIND_DETECTOR_MSG + detectorId, exception); });
    }

    private void profileModels(
        String detectorId,
        Set<ProfileName> profiles,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profiles, dataNodes);
        client.execute(ProfileAction.INSTANCE, profileRequest, onModelResponse(detectorId, profiles, listener));
    }

    private ActionListener<ProfileResponse> onModelResponse(
        String detectorId,
        Set<ProfileName> profiles,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        return ActionListener.wrap(profileResponse -> {
            DetectorProfile.Builder profile = new DetectorProfile.Builder();
            if (profiles.contains(ProfileName.COORDINATING_NODE)) {
                profile.coordinatingNode(profileResponse.getCoordinatingNode());
            }
            if (profiles.contains(ProfileName.SHINGLE_SIZE)) {
                profile.shingleSize(profileResponse.getShingleSize());
            }
            if (profiles.contains(ProfileName.TOTAL_SIZE_IN_BYTES)) {
                profile.totalSizeInBytes(profileResponse.getTotalSizeInBytes());
            }
            if (profiles.contains(ProfileName.MODELS)) {
                profile.modelProfile(profileResponse.getModelProfile());
            }

            listener.onResponse(profile.build());
        }, listener::onFailure);
    }

    private ActionListener<RCFPollingResponse> onPollRCFUpdates(
        String detectorId,
        Set<ProfileName> profilesToCollect,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        long enabledTime
    ) {
        return ActionListener.wrap(rcfPollResponse -> {
            long totalUpdates = rcfPollResponse.getTotalUpdates();
            if (totalUpdates == 0) {
                // no totalUpdates found maybe because either cold start hasn't started/finished or the first shingle
                // is not ready to trigger model initialization among nodes
                GetRequest getRequest = new GetRequest(DetectorInternalState.DETECTOR_STATE_INDEX, detectorId);
                client.get(getRequest, onGetEnabledDetectorState(listener, detectorId, enabledTime, profilesToCollect));
            } else if (totalUpdates < requiredSamples) {
                if (profilesToCollect.contains(ProfileName.STATE)) {
                    listener.onResponse(new DetectorProfile.Builder().state(DetectorState.INIT).build());
                }
                if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
                    GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
                    client
                        .get(
                            getDetectorRequest,
                            onGetDetectorForInitProgress(listener, detectorId, profilesToCollect, totalUpdates, requiredSamples)
                        );
                }
            } else {
                if (profilesToCollect.contains(ProfileName.STATE)) {
                    listener.onResponse(new DetectorProfile.Builder().state(DetectorState.RUNNING).build());
                }

                if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
                    InitProgressProfile initProgress = new InitProgressProfile("100%", 0, 0);
                    listener.onResponse(new DetectorProfile.Builder().initProgress(initProgress).build());
                }
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                // detector state index is not created yet
                respondEmptyInitProfile(listener, profilesToCollect);
            } else {
                logger.error(new ParameterizedMessage("Fail to get state and init progress for {}", detectorId), exception);
                listener.failImmediately(FAIL_TO_GET_PROFILE_MSG + detectorId, exception);
            }
        });
    }
}
