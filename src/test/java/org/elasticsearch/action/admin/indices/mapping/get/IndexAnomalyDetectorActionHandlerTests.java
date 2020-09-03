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

package org.elasticsearch.action.admin.indices.mapping.get;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;

/**
 *
 * we need to put the test in the same package of GetFieldMappingsResponse
 * (org.elasticsearch.action.admin.indices.mapping.get) since its constructor is
 * package private
 *
 */
public class IndexAnomalyDetectorActionHandlerTests extends AbstractADTest {
    static ThreadPool threadPool;
    private String TEXT_FIELD_TYPE = "text";
    private IndexAnomalyDetectorActionHandler handler;
    private ClusterService clusterService;
    private NodeClient clientMock;
    private RestChannel channel;
    private AnomalyDetectionIndices anomalyDetectionIndices;
    private String detectorId;
    private Long seqNo;
    private Long primaryTerm;
    private AnomalyDetector detector;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private TimeValue requestTimeout;
    private Integer maxSingleEntityAnomalyDetectors;
    private Integer maxMultiEntityAnomalyDetectors;
    private Integer maxAnomalyFeatures;
    private Settings settings;

    /**
     * Mockito does not allow mock final methods.  Make my own delegates and mock them.
     *
     */
    class NodeClientDelegate extends NodeClient {

        NodeClientDelegate(Settings settings, ThreadPool threadPool) {
            super(settings, threadPool);
        }

        public <Request extends ActionRequest, Response extends ActionResponse> void execute2(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            super.execute(action, request, listener);
        }

    }

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("IndexAnomalyDetectorJobActionHandlerTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        settings = Settings.EMPTY;
        clusterService = mock(ClusterService.class);
        clientMock = spy(new NodeClient(settings, null));

        channel = mock(RestChannel.class);

        final RestRequest restRequest = createRestRequest(Method.POST);

        when(channel.request()).thenReturn(restRequest);
        when(channel.newErrorBuilder()).thenReturn(JsonXContent.contentBuilder());
        when(channel.detailedErrorsEnabled()).thenReturn(true);

        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        when(anomalyDetectionIndices.doesAnomalyDetectorIndexExist()).thenReturn(true);

        detectorId = "123";
        seqNo = 0L;
        primaryTerm = 0L;

        WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;

        String field = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        requestTimeout = new TimeValue(1000L);

        maxSingleEntityAnomalyDetectors = 1000;

        maxMultiEntityAnomalyDetectors = 10;

        maxAnomalyFeatures = 5;

        handler = new IndexAnomalyDetectorActionHandler(
            settings,
            clusterService,
            clientMock,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures
        );
    }

    private SearchHits createSearchHits(int totalHits) {
        List<SearchHit> hitList = new ArrayList<>();
        IntStream.range(0, totalHits).forEach(i -> hitList.add(new SearchHit(i)));
        SearchHit[] hitArray = new SearchHit[hitList.size()];
        return new SearchHits(hitList.toArray(hitArray), new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1.0F);
    }

    public void testTwoCategoricalFields() throws IOException {
        expectThrows(
            IllegalArgumentException.class,
            () -> TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a", "b"))
        );
    }

    @SuppressWarnings("unchecked")
    public void testNoCategoricalField() throws IOException {
        SearchResponse mockResponse = mock(SearchResponse.class);
        int totalHits = 1001;
        when(mockResponse.getHits()).thenReturn(createSearchHits(totalHits));
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);

            assertTrue(args[0] instanceof SearchRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            listener.onResponse(mockResponse);

            return null;
        }).when(clientMock).search(any(SearchRequest.class), any());

        handler = new IndexAnomalyDetectorActionHandler(
            settings,
            clusterService,
            clientMock,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            // no categorical feature
            TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true),
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures
        );

        handler.start();
        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);
        verify(clientMock, never()).execute(eq(GetMappingsAction.INSTANCE), any(), any());
        verify(channel).sendResponse(response.capture());
        BytesRestResponse value = response.getValue();
        assertEquals(RestStatus.BAD_REQUEST, value.status());
        assertTrue(
            value.content().utf8ToString().contains(IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG)
        );
    }

    @SuppressWarnings("unchecked")
    public void testTextField() throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(createSearchHits(totalHits));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(SearchAction.INSTANCE)) {
                        listener.onResponse((Response) detectorResponse);
                    } else {
                        // we need to put the test in the same package of GetFieldMappingsResponse since its constructor is package private
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, TEXT_FIELD_TYPE)
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        handler = new IndexAnomalyDetectorActionHandler(
            settings,
            clusterService,
            client,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures
        );

        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);

        handler.start();

        verify(channel).sendResponse(response.capture());
        BytesRestResponse value = response.getValue();
        assertEquals(RestStatus.BAD_REQUEST, value.status());
        assertTrue(value.content().utf8ToString().contains(IndexAnomalyDetectorActionHandler.CATEGORICAL_FIELD_TYPE_ERR_MSG));
    }

    @SuppressWarnings("unchecked")
    private void testValidTypeTepmlate(String filedTypeName) throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(createSearchHits(totalHits));

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 0;
        when(userIndexResponse.getHits()).thenReturn(createSearchHits(userIndexHits));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(SearchAction.INSTANCE)) {
                        assertTrue(request instanceof SearchRequest);
                        SearchRequest searchRequest = (SearchRequest) request;
                        if (searchRequest.indices()[0].equals(ANOMALY_DETECTORS_INDEX)) {
                            listener.onResponse((Response) detectorResponse);
                        } else {
                            listener.onResponse((Response) userIndexResponse);
                        }
                    } else {

                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, filedTypeName)
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        NodeClient clientSpy = spy(client);

        handler = new IndexAnomalyDetectorActionHandler(
            settings,
            clusterService,
            clientSpy,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures
        );

        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);

        handler.start();

        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(channel).sendResponse(response.capture());
        BytesRestResponse value = response.getValue();
        assertEquals(RestStatus.BAD_REQUEST, value.status());
        assertTrue(value.content().utf8ToString().contains(IndexAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG));
    }

    public void testIpField() throws IOException {
        testValidTypeTepmlate(CommonName.IP_TYPE);
    }

    public void testKeywordField() throws IOException {
        testValidTypeTepmlate(CommonName.KEYWORD_TYPE);
    }

    @SuppressWarnings("unchecked")
    private void testUpdateTepmlate(String fieldTypeName) throws IOException {
        final RestRequest restRequest = createRestRequest(Method.PUT);
        when(channel.request()).thenReturn(restRequest);

        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(createSearchHits(totalHits));

        GetResponse getDetectorResponse = mock(GetResponse.class);
        when(getDetectorResponse.isExists()).thenReturn(true);

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 0;
        when(userIndexResponse.getHits()).thenReturn(createSearchHits(userIndexHits));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(SearchAction.INSTANCE)) {
                        assertTrue(request instanceof SearchRequest);
                        SearchRequest searchRequest = (SearchRequest) request;
                        if (searchRequest.indices()[0].equals(ANOMALY_DETECTORS_INDEX)) {
                            listener.onResponse((Response) detectorResponse);
                        } else {
                            listener.onResponse((Response) userIndexResponse);
                        }
                    } else if (action.equals(GetAction.INSTANCE)) {
                        assertTrue(request instanceof GetRequest);
                        listener.onResponse((Response) getDetectorResponse);
                    } else {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, fieldTypeName)
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        NodeClient clientSpy = spy(client);
        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        handler = new IndexAnomalyDetectorActionHandler(
            settings,
            clusterService,
            clientSpy,
            channel,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures
        );

        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);

        handler.start();

        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(channel).sendResponse(response.capture());
        BytesRestResponse value = response.getValue();
        assertEquals(RestStatus.BAD_REQUEST, value.status());
        if (fieldTypeName.equals(CommonName.IP_TYPE) || fieldTypeName.equals(CommonName.KEYWORD_TYPE)) {
            assertTrue(value.content().utf8ToString().contains(IndexAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG));
        } else {
            assertTrue(value.content().utf8ToString().contains(IndexAnomalyDetectorActionHandler.CATEGORICAL_FIELD_TYPE_ERR_MSG));
        }

    }

    public void testUpdateIpField() throws IOException {
        testUpdateTepmlate(CommonName.IP_TYPE);
    }

    public void testUpdateKeywordField() throws IOException {
        testUpdateTepmlate(CommonName.KEYWORD_TYPE);
    }

    public void testUpdateTextField() throws IOException {
        testUpdateTepmlate(TEXT_FIELD_TYPE);
    }

    @SuppressWarnings("unchecked")
    public void testMoreThanTenMultiEntityDetectors() throws IOException {
        SearchResponse mockResponse = mock(SearchResponse.class);

        int totalHits = 11;

        when(mockResponse.getHits()).thenReturn(createSearchHits(totalHits));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 2);

            assertTrue(args[0] instanceof SearchRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            listener.onResponse(mockResponse);

            return null;
        }).when(clientMock).search(any(SearchRequest.class), any());

        handler.start();

        ArgumentCaptor<BytesRestResponse> response = ArgumentCaptor.forClass(BytesRestResponse.class);
        verify(clientMock, times(1)).search(any(SearchRequest.class), any());
        verify(channel).sendResponse(response.capture());
        BytesRestResponse value = response.getValue();
        assertEquals(RestStatus.BAD_REQUEST, value.status());
        assertTrue(
            value.content().utf8ToString().contains(IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG)
        );
    }
}
