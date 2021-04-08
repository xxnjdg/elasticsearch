/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A module that binds the provided settings to the {@link Settings} interface.
 */
public class SettingsModule implements Module {
    private static final Logger logger = LogManager.getLogger(SettingsModule.class);

    private final Settings settings;
    //settings合集 = ClusterSettings.BUILT_IN_CLUSTER_SETTINGS 和 IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
    //构造方法闯入的 additionalSettings
    // 包含 Property.Filtered 属性 settings合集 的 key 和 各种插件 getSettingsFilter 方法获取
    private final Set<String> settingsFilterPattern = new HashSet<>();
    // 包含 Property.NodeScope 属性 settings合集
    private final Map<String, Setting<?>> nodeSettings = new HashMap<>();
    // 包含 Property.IndexScope 属性 settings合集
    private final Map<String, Setting<?>> indexSettings = new HashMap<>();
    // 包含 Property.NodeScope 和 Property.Consistent  属性 settings合集
    private final Set<Setting<?>> consistentSettings = new HashSet<>();
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;

    public SettingsModule(Settings settings, Setting<?>... additionalSettings) {
        this(settings, Arrays.asList(additionalSettings), Collections.emptyList(), Collections.emptySet());
    }

    //settingsFilter = 各种插件 getSettingsFilter 方法获取
    //settingUpgraders = 各种插件 getSettingUpgraders 方法获取
    //additionalSettings = 各种插件 getSettings 方法 + org.elasticsearch.threadpool.ThreadPool.builders.getRegisteredSettings() 方法返回
    public SettingsModule(
            Settings settings,
            List<Setting<?>> additionalSettings,
            List<String> settingsFilter,
            Set<SettingUpgrader<?>> settingUpgraders) {
        this.settings = settings;
        for (Setting<?> setting : ClusterSettings.BUILT_IN_CLUSTER_SETTINGS) {
            registerSetting(setting);
        }
        for (Setting<?> setting : IndexScopedSettings.BUILT_IN_INDEX_SETTINGS) {
            registerSetting(setting);
        }

        for (Setting<?> setting : additionalSettings) {
            registerSetting(setting);
        }
        for (String filter : settingsFilter) {
            registerSettingsFilter(filter);
        }
        final Set<SettingUpgrader<?>> clusterSettingUpgraders = new HashSet<>();
        for (final SettingUpgrader<?> settingUpgrader : ClusterSettings.BUILT_IN_SETTING_UPGRADERS) {
            assert settingUpgrader.getSetting().hasNodeScope() : settingUpgrader.getSetting().getKey();
            final boolean added = clusterSettingUpgraders.add(settingUpgrader);
            assert added : settingUpgrader.getSetting().getKey();
        }
        for (final SettingUpgrader<?> settingUpgrader : settingUpgraders) {
            assert settingUpgrader.getSetting().hasNodeScope() : settingUpgrader.getSetting().getKey();
            final boolean added = clusterSettingUpgraders.add(settingUpgrader);
            assert added : settingUpgrader.getSetting().getKey();
        }
        this.indexScopedSettings = new IndexScopedSettings(settings, new HashSet<>(this.indexSettings.values()));
        //clusterSettingUpgraders = ClusterSettings.BUILT_IN_SETTING_UPGRADERS 和 settingUpgraders 合并
        this.clusterSettings = new ClusterSettings(settings, new HashSet<>(this.nodeSettings.values()), clusterSettingUpgraders);
        Settings indexSettings = settings.filter((s) -> (s.startsWith("index.") &&
            // special case - we want to get Did you mean indices.query.bool.max_clause_count
            // which means we need to by-pass this check for this setting
            // TODO remove in 6.0!!
            "index.query.bool.max_clause_count".equals(s) == false)
            && clusterSettings.get(s) == null);
        if (indexSettings.isEmpty() == false) {
            try {
                String separator = IntStream.range(0, 85).mapToObj(s -> "*").collect(Collectors.joining("")).trim();
                StringBuilder builder = new StringBuilder();
                builder.append(System.lineSeparator());
                builder.append(separator);
                builder.append(System.lineSeparator());
                builder.append("Found index level settings on node level configuration.");
                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                int count = 0;
                for (String word : ("Since elasticsearch 5.x index level settings can NOT be set on the nodes configuration like " +
                    "the elasticsearch.yaml, in system properties or command line arguments." +
                    "In order to upgrade all indices the settings must be updated via the /${index}/_settings API. " +
                    "Unless all settings are dynamic all indices must be closed in order to apply the upgrade" +
                    "Indices created in the future should use index templates to set default values."
                ).split(" ")) {
                    if (count + word.length() > 85) {
                        builder.append(System.lineSeparator());
                        count = 0;
                    }
                    count += word.length() + 1;
                    builder.append(word).append(" ");
                }

                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                builder.append("Please ensure all required values are updated on all indices by executing: ");
                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                builder.append("curl -XPUT 'http://localhost:9200/_all/_settings?preserve_existing=true' -d '");
                try (XContentBuilder xContentBuilder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    xContentBuilder.prettyPrint();
                    xContentBuilder.startObject();
                    indexSettings.toXContent(xContentBuilder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
                    xContentBuilder.endObject();
                    builder.append(Strings.toString(xContentBuilder));
                }
                builder.append("'");
                builder.append(System.lineSeparator());
                builder.append(separator);
                builder.append(System.lineSeparator());

                logger.warn(builder.toString());
                throw new IllegalArgumentException("node settings must not contain any index level settings");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // by now we are fully configured, lets check node level settings for unregistered index settings
        clusterSettings.validate(settings, true);
        //初始化
        this.settingsFilter = new SettingsFilter(settingsFilterPattern);
     }

    @Override
    public void configure(Binder binder) {
        binder.bind(Settings.class).toInstance(settings);
        binder.bind(SettingsFilter.class).toInstance(settingsFilter);
        binder.bind(ClusterSettings.class).toInstance(clusterSettings);
        binder.bind(IndexScopedSettings.class).toInstance(indexScopedSettings);
    }

    /**
     * Registers a new setting. This method should be used by plugins in order to expose any custom settings the plugin defines.
     * Unless a setting is registered the setting is unusable. If a setting is never the less specified the node will reject
     * the setting during startup.
     */
    private void registerSetting(Setting<?> setting) {
        if (setting.isFiltered()) {
            if (settingsFilterPattern.contains(setting.getKey()) == false) {
                //注册
                registerSettingsFilter(setting.getKey());
            }
        }
        if (setting.hasNodeScope() || setting.hasIndexScope()) {
            if (setting.hasNodeScope()) {
                Setting<?> existingSetting = nodeSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                if (setting.isConsistent()) {
                    if (setting instanceof Setting.AffixSetting<?>) {
                        if (((Setting.AffixSetting<?>)setting).getConcreteSettingForNamespace("_na_") instanceof SecureSetting<?>) {
                            consistentSettings.add(setting);
                        } else {
                            throw new IllegalArgumentException("Invalid consistent secure setting [" + setting.getKey() + "]");
                        }
                    } else if (setting instanceof SecureSetting<?>) {
                        consistentSettings.add(setting);
                    } else {
                        throw new IllegalArgumentException("Invalid consistent secure setting [" + setting.getKey() + "]");
                    }
                }
                nodeSettings.put(setting.getKey(), setting);
            }
            if (setting.hasIndexScope()) {
                Setting<?> existingSetting = indexSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                if (setting.isConsistent()) {
                    throw new IllegalStateException("Consistent setting [" + setting.getKey() + "] cannot be index scoped");
                }
                indexSettings.put(setting.getKey(), setting);
            }
        } else {
            throw new IllegalArgumentException("No scope found for setting [" + setting.getKey() + "]");
        }
    }

    /**
     * Registers a settings filter pattern that allows to filter out certain settings that for instance contain sensitive information
     * or if a setting is for internal purposes only. The given pattern must either be a valid settings key or a simple regexp pattern.
     */
    private void registerSettingsFilter(String filter) {
        if (SettingsFilter.isValidPattern(filter) == false) {
            throw new IllegalArgumentException("filter [" + filter +"] is invalid must be either a key or a regex pattern");
        }
        if (settingsFilterPattern.contains(filter)) {
            throw new IllegalArgumentException("filter [" + filter + "] has already been registered");
        }
        settingsFilterPattern.add(filter);
    }

    public Settings getSettings() {
        return settings;
    }

    public IndexScopedSettings getIndexScopedSettings() {
        return indexScopedSettings;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public Set<Setting<?>> getConsistentSettings() {
        return consistentSettings;
    }

    public SettingsFilter getSettingsFilter() {
        return settingsFilter;
    }

}
