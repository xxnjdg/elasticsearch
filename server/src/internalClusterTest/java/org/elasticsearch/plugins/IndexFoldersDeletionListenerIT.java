/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.elasticsearch.env.NodeEnvironment.INDICES_FOLDER;
import static org.elasticsearch.gateway.MetadataStateFormat.STATE_DIR_NAME;
import static org.elasticsearch.index.shard.ShardPath.INDEX_FOLDER_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndexFoldersDeletionListenerIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(IndexFoldersDeletionListenerPlugin.class);
        return plugins;
    }

    public void testListenersInvokedWhenIndexIsDeleted() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final NumShards numShards = getNumShards(indexName);
        ensureClusterSizeConsistency(); // wait for a stable cluster
        ensureGreen(indexName); // wait for no relocation

        final ClusterState clusterState = clusterService().state();
        final Index index = clusterState.metadata().index(indexName).getIndex();
        final Map<String, List<ShardRouting>> shardsByNodes = shardRoutingsByNodes(clusterState, index);
        assertThat(shardsByNodes.values().stream().mapToInt(List::size).sum(), equalTo(numShards.totalNumShards));

        for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
            final String nodeName = shardsByNode.getKey();
            final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);
            assertTrue("Expecting no indices deleted on node " + nodeName, plugin.deletedIndices.isEmpty());
            assertTrue("Expecting no shards deleted on node " + nodeName, plugin.deletedShards.isEmpty());
        }

        assertAcked(client().admin().indices().prepareDelete(indexName));

        assertBusy(() -> {
            for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
                final String nodeName = shardsByNode.getKey();
                final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);
                assertTrue("Listener should have been notified of deletion of index " + index + " on node " + nodeName,
                    plugin.deletedIndices.contains(index));

                final List<ShardId> deletedShards = plugin.deletedShards.get(index);
                assertThat(deletedShards, notNullValue());
                assertFalse("Listener should have been notified of deletion of one or more shards on node " + nodeName,
                    deletedShards.isEmpty());

                for (ShardRouting shardRouting : shardsByNode.getValue()) {
                    final ShardId shardId = shardRouting.shardId();
                    assertTrue("Listener should have been notified of deletion of shard " + shardId + " on node " + nodeName,
                        deletedShards.contains(shardId));
                }
            }
        });
    }

    public void testListenersInvokedWhenIndexIsRelocated() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(4, 10))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 1))
            .build());

        final NumShards numShards = getNumShards(indexName);
        ensureGreen(indexName);

        final ClusterState clusterState = clusterService().state();
        final Index index = clusterState.metadata().index(indexName).getIndex();
        final Map<String, List<ShardRouting>> shardsByNodes = shardRoutingsByNodes(clusterState, index);
        assertThat(shardsByNodes.values().stream().mapToInt(List::size).sum(), equalTo(numShards.totalNumShards));

        for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
            final String nodeName = shardsByNode.getKey();
            final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);
            assertTrue("Expecting no indices deleted on node " + nodeName, plugin.deletedIndices.isEmpty());
            assertTrue("Expecting no shards deleted on node " + nodeName, plugin.deletedShards.isEmpty());
        }

        final List<String> excludedNodes = randomSubsetOf(2, shardsByNodes.keySet());
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder()
                .put("index.routing.allocation.exclude._name", String.join(",", excludedNodes))
                .build()));
        ensureGreen(indexName);

        assertBusy(() -> {
            for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
                final String nodeName = shardsByNode.getKey();
                final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);

                if (excludedNodes.contains(nodeName)) {
                    assertTrue("Listener should have been notified of deletion of index " + index + " on node " + nodeName,
                        plugin.deletedIndices.contains(index));

                    final List<ShardId> deletedShards = plugin.deletedShards.get(index);
                    assertThat(deletedShards, notNullValue());
                    assertFalse("Listener should have been notified of deletion of one or more shards on node " + nodeName,
                        deletedShards.isEmpty());

                    for (ShardRouting shardRouting : shardsByNode.getValue()) {
                        final ShardId shardId = shardRouting.shardId();
                        assertTrue("Listener should have been notified of deletion of shard " + shardId + " on node " + nodeName,
                            deletedShards.contains(shardId));
                    }
                } else {
                    assertTrue("Expecting no indices deleted on node " + nodeName, plugin.deletedIndices.isEmpty());
                    assertTrue("Expecting no shards deleted on node " + nodeName, plugin.deletedShards.isEmpty());
                }
            }
        });
    }

    public void testListenersInvokedWhenIndexIsDangling() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(4, 10))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 1))
            .build());

        final NumShards numShards = getNumShards(indexName);
        ensureGreen(indexName);

        final ClusterState clusterState = clusterService().state();
        final Index index = clusterState.metadata().index(indexName).getIndex();
        final Map<String, List<ShardRouting>> shardsByNodes = shardRoutingsByNodes(clusterState, index);
        assertThat(shardsByNodes.values().stream().mapToInt(List::size).sum(), equalTo(numShards.totalNumShards));

        for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
            final String nodeName = shardsByNode.getKey();
            final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);
            assertTrue("Expecting no indices deleted on node " + nodeName, plugin.deletedIndices.isEmpty());
            assertTrue("Expecting no shards deleted on node " + nodeName, plugin.deletedShards.isEmpty());
        }

        final String stoppedNode = randomFrom(shardsByNodes.keySet());
        final Settings stoppedNodeDataPathSettings = internalCluster().dataPathSettings(stoppedNode);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(stoppedNode));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final String restartedNode = internalCluster().startNode(stoppedNodeDataPathSettings);
        assertBusy(() -> {
            final IndexFoldersDeletionListenerPlugin plugin = plugin(restartedNode);
            assertTrue("Listener should have been notified of deletion of index " + index + " on node " + restartedNode,
                plugin.deletedIndices.contains(index));
        });
    }

    public void testListenersInvokedWhenIndexHasLeftOverShard() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();

        final Path dataDirWithLeftOverShards = createTempDir();
        String dataNode = internalCluster().startDataOnlyNode(
            Settings.builder()
                .putList(Environment.PATH_DATA_SETTING.getKey(), singletonList(dataDirWithLeftOverShards.toAbsolutePath().toString()))
                .putNull(Environment.PATH_SHARED_DATA_SETTING.getKey())
                .build()
        );

        final Index[] leftovers = new Index[between(1, 3)];
        logger.debug("--> creating [{}] leftover indices on data node [{}]", leftovers.length, dataNode);
        for (int i = 0; i < leftovers.length; i++) {
            final String indexName = "index-" + i;
            createIndex(indexName, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", dataNode)
                .build());
            ensureGreen(indexName);
            leftovers[i] = internalCluster().clusterService(masterNode).state().metadata().index(indexName).getIndex();
        }

        logger.debug("--> stopping data node [{}], the data left on disk will be injected as left-overs in a newer data node", dataNode);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataNode));

        logger.debug("--> deleting leftover indices");
        assertAcked(client().admin().indices().prepareDelete("index-*"));

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        logger.debug("--> creating a new index [{}]", indexName);
        assertAcked(client().admin().indices().prepareCreate(indexName).setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.enable", EnableAllocationDecider.Allocation.NONE)
                .build())
            .setWaitForActiveShards(ActiveShardCount.NONE));

        final Index index = internalCluster().clusterService(masterNode).state().metadata().index(indexName).getIndex();
        logger.debug("--> index [{}] created", index);

        final List<Path> dataPaths = new ArrayList<>();
        for (int i = 0; i < leftovers.length; i++) {
            final Path dataPath = createTempDir();
            dataPaths.add(dataPath);
            final Path shardPath = dataPath.resolve("nodes").resolve("0").resolve(INDICES_FOLDER).resolve(index.getUUID()).resolve("0");
            Files.createDirectories(shardPath);
            final Path leftoverPath = dataDirWithLeftOverShards.resolve("nodes").resolve("0").resolve(INDICES_FOLDER)
                .resolve(leftovers[i].getUUID()).resolve("0");
            Files.move(leftoverPath.resolve(STATE_DIR_NAME), shardPath.resolve(STATE_DIR_NAME));
            Files.move(leftoverPath.resolve(INDEX_FOLDER_NAME), shardPath.resolve(INDEX_FOLDER_NAME));
        }

        logger.debug("--> starting another data node with data paths [{}]", dataPaths);
        dataNode = internalCluster().startDataOnlyNode(
            Settings.builder()
                .putList(Environment.PATH_DATA_SETTING.getKey(),
                    dataPaths.stream().map(p -> p.toAbsolutePath().toString()).collect(Collectors.toList()))
                .putNull(Environment.PATH_SHARED_DATA_SETTING.getKey())
                .build());

        final IndexFoldersDeletionListenerPlugin plugin = plugin(dataNode);
        assertTrue("Expecting no shards deleted on node " + dataNode, plugin.deletedShards.isEmpty());

        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
            .put("index.routing.allocation.enable", EnableAllocationDecider.Allocation.ALL)
            .put("index.routing.allocation.require._name", dataNode)
        ));
        ensureGreen(indexName);

        assertTrue("Listener should have been notified of deletion of left-over shards for index " + index + " on node " + dataNode,
            plugin.deletedShards.containsKey(index));
        assertThat("Listener should have been notified of [" + leftovers.length + "] deletions of left-over shard [0] on node " + dataNode,
            plugin.deletedShards.get(index).size(), equalTo(leftovers.length));
    }

    private Map<String, List<ShardRouting>> shardRoutingsByNodes(ClusterState clusterState, Index index) {
        final Map<String, List<ShardRouting>> map = new HashMap<>();
        for (ShardRouting shardRouting : clusterState.routingTable().index(index).shardsWithState(ShardRoutingState.STARTED)) {
            final String nodeName = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
            map.computeIfAbsent(nodeName, name -> new ArrayList<>()).add(shardRouting);
        }
        return map;
    }

    public static class IndexFoldersDeletionListenerPlugin extends Plugin implements IndexStorePlugin {

        final Set<Index> deletedIndices = ConcurrentCollections.newConcurrentSet();
        final Map<Index, List<ShardId>> deletedShards = ConcurrentCollections.newConcurrentMap();

        @Override
        public List<IndexFoldersDeletionListener> getIndexFoldersDeletionListeners() {
            return singletonList(new IndexFoldersDeletionListener() {
                @Override
                public void beforeIndexFoldersDeleted(Index index, IndexSettings indexSettings, Path[] indexPaths) {
                    deletedIndices.add(index);
                }

                @Override
                public void beforeShardFoldersDeleted(ShardId shardId, IndexSettings indexSettings, Path[] shardPaths) {
                    deletedShards.computeIfAbsent(shardId.getIndex(), i -> new ArrayList<>()).add(shardId);
                }
            });
        }

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.emptyMap();
        }
    }

    private static IndexFoldersDeletionListenerPlugin plugin(String nodeId) {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, nodeId);
        final List<IndexFoldersDeletionListenerPlugin> plugins = pluginsService.filterPlugins(IndexFoldersDeletionListenerPlugin.class);
        assertThat(plugins, hasSize(1));
        return plugins.get(0);
    }

    private static void assertPendingDeletesProcessed() throws Exception {
        assertBusy(() -> {
            final Iterable<IndicesService> services = internalCluster().getDataNodeInstances(IndicesService.class);
            services.forEach(indicesService -> assertFalse(indicesService.hasUncompletedPendingDeletes()));
        });
    }
}