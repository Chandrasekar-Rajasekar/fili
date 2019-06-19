// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

import com.yahoo.bard.webservice.config.luthier.factories.KeyValueStoreDimensionFactory;
import com.yahoo.bard.webservice.config.luthier.factories.LuceneSearchProviderFactory;
import com.yahoo.bard.webservice.config.luthier.factories.MapKeyValueStoreFactory;
import com.yahoo.bard.webservice.config.luthier.factories.NoOpSearchProviderFactory;
import com.yahoo.bard.webservice.config.luthier.factories.PermissivePhysicalTableFactory;
import com.yahoo.bard.webservice.config.luthier.factories.ScanSearchProviderFactory;
import com.yahoo.bard.webservice.config.luthier.factories.StrictPhysicalTableFactory;
import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.LuthierResourceDictionaries;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Dependency Injection container for Config Objects configured via Luthier.
 */
public class LuthierIndustrialPark implements ConfigurationLoader {

    private final LuthierResourceDictionaries resourceDictionaries;
    private final DataSourceMetadataService metadataService;

    private final FactoryPark<Dimension> dimensionFactoryPark;
    private final FactoryPark<SearchProvider> searchProviderFactoryPark;
    private final FactoryPark<KeyValueStore> keyValueStoreFactoryPark;

    private final FactoryPark<MetricMaker> metricMakerFactoryPark;

    private final FactoryPark<ConfigPhysicalTable> physicalTableFactoryPark;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to initialize the industrial park with
     * @param conceptFactoryMap  A collection of named factories, partitioned by concept types
     */
    protected LuthierIndustrialPark(
            LuthierResourceDictionaries resourceDictionaries,
            Map<ConceptType<?>, Map<String, ? extends Factory<? extends Object>>> conceptFactoryMap
    ) {

        this.resourceDictionaries = resourceDictionaries;

        this.searchProviderFactoryPark = buildFactoryPark(ConceptType.SEARCH_PROVIDER, conceptFactoryMap);
        this.keyValueStoreFactoryPark = buildFactoryPark(ConceptType.KEY_VALUE_STORE, conceptFactoryMap);
        this.dimensionFactoryPark = buildFactoryPark(ConceptType.DIMENSION, conceptFactoryMap);

        this.metricMakerFactoryPark =  buildFactoryPark(ConceptType.METRIC_MAKER, conceptFactoryMap);
        this.physicalTableFactoryPark = buildFactoryPark(ConceptType.PHYSICAL_TABLE, conceptFactoryMap);

        this.metadataService = new DataSourceMetadataService();

    }

    /**
     * For a given collection of factories, build a single FactoryPark.
     *
     * @param concept  The concept of the factory map being created.
     * @param conceptFactoryMap  A collection of named factories, partitioned by concept types
     * @param <T> The type of the entity corresponding to this factory map.
     *
     * @return  A Factory Park defined for a given set of named factories.
     */
    private static <T> FactoryPark<T> buildFactoryPark(
            ConceptType<T> concept,
            Map<ConceptType<?>, Map<String, ? extends Factory<? extends Object>>> conceptFactoryMap
    ) {
        Map<String, Factory<T>> factories = (Map<String, Factory<T>>) conceptFactoryMap.get(concept);

        return new FactoryPark<>(
                new ResourceNodeSupplier(concept.getResourceName()),
                factories
        );
    }

    /**
     * Retrieve or build a dimension.
     *
     * @param dimensionName the name for the dimension to be provided.
     *
     * @return the dimension instance corresponding to this name.
     */
    public Dimension getDimension(String dimensionName) {
        DimensionDictionary dimensionDictionary = resourceDictionaries.getDimensionDictionary();
        if (dimensionDictionary.findByApiName(dimensionName) == null) {
            Dimension dimension = dimensionFactoryPark.buildEntity(dimensionName, this);
            dimensionDictionary.add(dimension);
        }
        return dimensionDictionary.findByApiName(dimensionName);
    }

    /**
     * Retrieve or build a Metric Maker.
     *
     * @param metricMakerName the name for the dimension to be provided.
     *
     * @return the dimension instance corresponding to this name.
     */
    public MetricMaker getMetricMaker(String metricMakerName) {
        Map<String, MetricMaker> metricMakerDictionary = resourceDictionaries.getMetricMakerDictionary();
        if (metricMakerDictionary.get(metricMakerName) == null) {
            MetricMaker metricMaker = metricMakerFactoryPark.buildEntity(metricMakerName, this);
            metricMakerDictionary.put(metricMakerName, metricMaker);
        }
        return metricMakerDictionary.get(metricMakerName);
    }

    /**
     * Retrieve or build a SearchProvider.
     *
     * @param domain  a string that is associated with the space this provider
     * searches for. It will typically be the dimension name unless more than
     * one dimension shares the same SearchProvider.
     *
     * @return an instance of the SearchProvider that correspond to the domain
     */
    public SearchProvider getSearchProvider(String domain) {
        Map<String, SearchProvider> searchProviderDictionary = resourceDictionaries.getSearchProviderDictionary();
        if (!searchProviderDictionary.containsKey(domain)) {
            SearchProvider searchProvider = searchProviderFactoryPark.buildEntity(domain, this);
            searchProviderDictionary.put(domain, searchProvider);
        }
        return searchProviderDictionary.get(domain);
    }

    /**
     * Retrieve or build a KeyValueStore.
     *
     * @param domain identifier of the keyValueStore
     *
     * @return the keyValueStore built according to the keyValueStore identifier
     * @throws IllegalArgumentException when passed in redisStore.
     */
    public KeyValueStore getKeyValueStore(String domain) throws UnsupportedOperationException {
        Map<String, KeyValueStore> keyValueStoreDictionary = resourceDictionaries.getKeyValueStoreDictionary();
        if (! keyValueStoreDictionary.containsKey(domain)) {
            KeyValueStore keyValueStore = keyValueStoreFactoryPark.buildEntity(domain, this);
            keyValueStoreDictionary.put(domain, keyValueStore);
        }
        return keyValueStoreDictionary.get(domain);
    }

    /**
     * Retrieve or build a PhysicalTable.
     *
     * @param tableName the name for the PhysicalTable to be retrieved or built.
     *
     * @return the PhysicalTable instance corresponding to this name.
     */
    public ConfigPhysicalTable getPhysicalTable(String tableName) {
        PhysicalTableDictionary physicalTableDictionary = resourceDictionaries.getPhysicalDictionary();
        if (!physicalTableDictionary.containsKey(tableName)) {
            ConfigPhysicalTable physicalTable = physicalTableFactoryPark.buildEntity(tableName, this);
            physicalTableDictionary.put(tableName, physicalTable);
        }
        return physicalTableDictionary.get(tableName);
    }

    public DataSourceMetadataService getMetadataService() {
        return metadataService;
    }

    /**
     * Retrieve or build a metric.
     *
     * @param metricName  the name pf the LogicalMetric to be retrieved or built.
     *
     * @return the LogicalMetric instance corresponding to this metricName.
     */
    // TODO: to be added after metric makers are working
    public LogicalMetric getMetric(String metricName) {
        return null;
    }

    @Override
    public void load() {
        dimensionFactoryPark.fetchConfig().fieldNames().forEachRemaining(this::getDimension);
        physicalTableFactoryPark.fetchConfig().fieldNames().forEachRemaining(this::getPhysicalTable);
    }

    @Override
    public DimensionDictionary getDimensionDictionary() {
        return resourceDictionaries.getDimensionDictionary();
    }

    @Override
    public MetricDictionary getMetricDictionary() {
        return resourceDictionaries.getMetricDictionary();
    }

    @Override
    public LogicalTableDictionary getLogicalTableDictionary() {
        return resourceDictionaries.getLogicalDictionary();
    }

    @Override
    public PhysicalTableDictionary getPhysicalTableDictionary() {
        return resourceDictionaries.getPhysicalDictionary();
    }

    @Override
    public ResourceDictionaries getDictionaries() {
        return resourceDictionaries;
    }

    /**
     * Builder object to construct a new LuthierIndustrialPark instance with.
     */
    public static class Builder {

        private Map<String, Factory<Dimension>> dimensionFactories;
        private Map<String, Factory<SearchProvider>> searchProviderFactories;
        private Map<String, Factory<KeyValueStore>> keyValueStoreFactories;
        private Map<String, Factory<ConfigPhysicalTable>> physicalTableFactories;
        private Map<ConceptType<?>, Map<String, ? extends Factory<? extends Object>>> conceptFactoryMap;

        private final LuthierResourceDictionaries resourceDictionaries;

        /**
         * Constructor.
         *
         * @param resourceDictionaries  a class that contains resource dictionaries including
         * PhysicalTableDictionary, DimensionDictionary, etc.
         */
        public Builder(LuthierResourceDictionaries resourceDictionaries) {
            this.resourceDictionaries = resourceDictionaries;
            conceptFactoryMap = new HashMap<>();
            conceptFactoryMap.put(ConceptType.METRIC_MAKER, new HashMap<>());
            conceptFactoryMap.put(ConceptType.DIMENSION, getDefaultDimensionFactories());
            conceptFactoryMap.put(ConceptType.SEARCH_PROVIDER, getDefaultSearchProviderFactories());
            conceptFactoryMap.put(ConceptType.PHYSICAL_TABLE, getDefaultPhysicalTableFactories());
        }

        /**
         * Constructor.
         * <p>
         * Default to use an empty resource dictionary.
         */
        public Builder() {
            this(new LuthierResourceDictionaries());
        }


        /**
         * Default dimension factories that currently lives in the code base.
         *
         * @return a LinkedHashMap of KeyValueStoreDimension to its factory
         */
        private Map<String, Factory<Dimension>> getDefaultDimensionFactories() {
            Map<String, Factory<Dimension>> dimensionFactoryMap = new LinkedHashMap<>();
            dimensionFactoryMap.put("KeyValueStoreDimension", new KeyValueStoreDimensionFactory());
            return dimensionFactoryMap;
        }

        /**
         * Default keyValueStore factories that currently lives in the code base.
         *
         * @return  a LinkedHashMap of KeyValueStore to its factory
         */
        private Map<String, Factory<KeyValueStore>> getDefaultKeyValueStoreFactories() {
            Map<String, Factory<KeyValueStore>> keyValueStoreFactoryMap = new LinkedHashMap<>();
            MapKeyValueStoreFactory mapStoreFactory = new MapKeyValueStoreFactory();
            keyValueStoreFactoryMap.put("memory", mapStoreFactory);
            keyValueStoreFactoryMap.put("map", mapStoreFactory);
            keyValueStoreFactoryMap.put("mapStore", mapStoreFactory);
            keyValueStoreFactoryMap.put("com.yahoo.bard.webservice.data.dimension.MapStore", mapStoreFactory);
            // TODO: add in Redis Store later
            return keyValueStoreFactoryMap;
        }

        /**
         * Default searchProvider factories that currently lives in the code base.
         *
         * @return a LinkedHashMap of aliases of search provider type name to its factory
         */
        private Map<String, Factory<SearchProvider>> getDefaultSearchProviderFactories() {
            Map<String, Factory<SearchProvider>> searchProviderFactoryMap = new LinkedHashMap<>();
            // all known factories for searchProviders and their possible aliases
            LuceneSearchProviderFactory luceneSearchProviderFactory = new LuceneSearchProviderFactory();
            List<String> luceneAliases = Arrays.asList(
                    "lucene",
                    LuceneSearchProvider.class.getSimpleName(),
                    LuceneSearchProvider.class.getCanonicalName()
            );
            NoOpSearchProviderFactory noOpSearchProviderFactory = new NoOpSearchProviderFactory();
            List<String> noOpAliases = Arrays.asList(
                    "noOp",
                    NoOpSearchProvider.class.getSimpleName(),
                    NoOpSearchProvider.class.getCanonicalName()
            );
            ScanSearchProviderFactory scanSearchProviderFactory = new ScanSearchProviderFactory();
            List<String> scanAliases = Arrays.asList(
                    "memory",
                    "scan",
                    ScanSearchProvider.class.getSimpleName(),
                    ScanSearchProvider.class.getCanonicalName()
            );
            luceneAliases.forEach(alias -> searchProviderFactoryMap.put(alias, luceneSearchProviderFactory));
            noOpAliases.forEach(alias -> searchProviderFactoryMap.put(alias, noOpSearchProviderFactory));
            scanAliases.forEach(alias -> searchProviderFactoryMap.put(alias, scanSearchProviderFactory));
            return searchProviderFactoryMap;
        }

        /**
         * Default physicalTable factories that currently lives in the code base.
         *
         * @return a LinkedHashMap of physicalTable type name to its factory
         */
        private Map<String, Factory<ConfigPhysicalTable>> getDefaultPhysicalTableFactories() {
            Map<String, Factory<ConfigPhysicalTable>> physicalTableFactoryMap = new LinkedHashMap<>();
            StrictPhysicalTableFactory strictFactory = new StrictPhysicalTableFactory();
            PermissivePhysicalTableFactory permissiveFactory = new PermissivePhysicalTableFactory();
            physicalTableFactoryMap.put("strictPhysicalTable", strictFactory);
            physicalTableFactoryMap.put("strict", strictFactory);
            physicalTableFactoryMap.put("permissivePhysicalTable", permissiveFactory);
            physicalTableFactoryMap.put("permissive", permissiveFactory);
            return physicalTableFactoryMap;
        }

        /**
         * Registers factories for a give concept.
         * <p>
         * There should be one factory per construction pattern for a concept in the system.
         *
         * @param conceptType The configuration concept being configured (e.g. Dimension, Metric..)
         * @param factories  A mapping from names of factories to a factory that builds instances of that type
         * @param <T> The configuration entity produced by this set of collection of factories.
         *
         * @return the builder object
         */
        public <T> Builder withFactories(ConceptType<T> conceptType, Map<String, Factory<T>> factories) {
            conceptFactoryMap.put(conceptType, factories);
            return this;
        }

        /**
         * Registers factories for a give concept.
         * <p>
         * There should be one factory per construction pattern for a concept in the system.
         *
         * @param conceptType The configuration concept being configured (e.g. Dimension, Metric..)
         * @param factories  A mapping from names of factories to a factory that builds instances of that type
         * @param <T> The configuration entity produced by this set of collection of factories.
         *
         * @return the builder object
         */
        @SuppressWarnings("unchecked")
        public <T> Builder addFactories(ConceptType<T> conceptType, Map<String, Factory<T>> factories) {
            Map<String, Factory<T>> factory = (Map<String, Factory<T>>) conceptFactoryMap.computeIfAbsent(
                    conceptType,
                    (ignore) -> new LinkedHashMap<>()
            );
            factory.putAll(factories);
            return this;
        }

        /**
         * Registers a named factory with the Industrial Park Builder.
         * <p>
         * There should be one factory per construction pattern for a concept in the system.
         *
         * @param conceptType  The configuration type for the entity being produced
         * @param name  The identifier used in the configuration to identify the type of
         * dimension built by this factory
         * @param factory  A factory that builds Dimensions of the type named by {@code name}
         * @param <T>  The configuration entity produced by this set of collection of factories
         *
         * @return the builder object
         */
        @SuppressWarnings("unchecked")
        public <T> Builder addFactory(ConceptType<T> conceptType, String name, Factory<T> factory) {
            Map<String, Factory<T>> conceptFactory = (Map<String, Factory<T>>) conceptFactoryMap.get(conceptType);
            conceptFactory.put(name, factory);
            return this;
        }

        /**
         * Registers named searchProvider factories with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of searchProvider used in the config
         *
         * @param factories  A mapping from a searchProvider type identifier used in the config
         * to a factory that builds SearchProvider of that type
         *
         * @return the builder object
         */
        public Builder withSearchProviderFactories(Map<String, Factory<SearchProvider>> factories) {
            this.searchProviderFactories = factories;
            return this;
        }

        /**
         * Registers named KeyValueStore factories with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of keyValueStore used in the config
         *
         * @param factories  A mapping from a keyValueStore type identifier used in the config
         * to a factory that builds keyValueStore of that type
         *
         * @return the builder object
         */
        public Builder withKeyValueStoreFactories(Map<String, Factory<KeyValueStore>> factories) {
            this.keyValueStoreFactories = factories;
            return this;
        }

        /**
         * Registers a named searchProvider factory with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of searchProvider used in the config
         *
         * @param name  The identifier used in the configuration to identify the type of
         * searchProvider built by this factory
         * @param factory  A factory that builds searchProvider of the type named by {@code name}
         *
         * @return the builder object
         */
        public Builder withSearchProviderFactory(String name, Factory<SearchProvider> factory) {
            searchProviderFactories.put(name, factory);
            return this;
        }

        /**
         * Registers named PhysicalTable factories with the Industrial Park Builder.
         * <p>
         * There should be one factory per type of physicalTable used in the config
         *
         * @param factories  A mapping from a PhysicalTable type identifier used in the config
         * to a factory that builds PhysicalTable of that type
         *
         * @return the builder object
         */
        public Builder withPhysicalTableFactories(Map<String, Factory<ConfigPhysicalTable>> factories) {
            this.physicalTableFactories = factories;
            return this;
        }

        /**
         * Builds a LuthierIndustrialPark.
         *
         * @return the LuthierIndustrialPark with the specified resourceDictionaries and factories
         */
        @SuppressWarnings("unchecked")
        public LuthierIndustrialPark build() {
            return new LuthierIndustrialPark(resourceDictionaries, conceptFactoryMap);
        }
    }
}
