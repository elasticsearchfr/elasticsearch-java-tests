package org.elasticsearchfr.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.impl.PointImpl;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearchfr.tests.bean.Beer;
import org.elasticsearchfr.tests.bean.BeerHelper;
import org.elasticsearchfr.tests.helper.TestNodeHelper;
import org.junit.*;

/**
 * We want to have an example for all existing filters listed on
 * <a href="http://www.elasticsearch.org/guide/reference/query-dsl/">Query DSL documentation</a>
 * @author David Pilato (aka dadoonet)
 */
public class ES006AllFiltersTest extends TestNodeHelper {
	protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

    /**
     * When we start a test, we index 1000 beers with random data
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        BulkRequestBuilder brb = node.client().prepareBulk();

        for (int i = 0; i < 1000; i++) {
            Beer beer = BeerHelper.generate();
            IndexRequest irq = new IndexRequest("meal", "beer", "beer_" + i);
            String jsonString = mapper.writeValueAsString(beer);
            irq.source(jsonString);
            brb.add(irq);
        }

        BulkResponse br = brb.execute().actionGet();
        Assert.assertFalse(br.hasFailures());

        node.client().admin().indices().prepareRefresh().execute().actionGet();
    }

    /**
     * When we stop a test, we remove all data
     */
    @After
    public void tearDown() {
        BulkRequestBuilder brb = node.client().prepareBulk();

        for (int i = 0; i < 1000; i++) {
            DeleteRequest dr = new DeleteRequest("meal", "beer", "beer_" + i);
            brb.add(dr);
        }

        BulkResponse br = brb.execute().actionGet();
        Assert.assertFalse(br.hasFailures());
    }

    /**
     * We want to build a And Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/and-filter.html">documentation</a>
     */
    @Test
    public void andFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("price").from("1").to("2"),
                FilterBuilders.prefixFilter("brand", "hei")
        );

        launchSearch(filter);
    }

    /**
     * We want to build a And Filter with Cache
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/and-filter.html">documentation</a>
     */
    @Test
    public void andWithCacheFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("price").from("1").to("2"),
                FilterBuilders.prefixFilter("brand", "hei")
            ).cache(true);

        launchSearch(filter);
    }

    /**
     * We want to build a Bool Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/bool-filter.html">documentation</a>
     */
    @Test
    public void boolFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.boolFilter()
            .must(FilterBuilders.termFilter("brand", "heineken"))
            .mustNot(FilterBuilders.rangeFilter("price").from("1").to("2"))
            .should(FilterBuilders.termFilter("colour", "dark"))
            .should(FilterBuilders.termFilter("colour", "pale"))
           ;

        launchSearch(filter);
    }

    /**
     * We want to build an Exists Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/exists-filter.html">documentation</a>
     */
    @Test
    public void existsFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.existsFilter("brand");

        launchSearch(filter);
    }

    /**
     * We want to build an Ids Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/ids-filter.html">documentation</a>
     */
    @Test
    public void idsFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.idsFilter("beer")
                .addIds("beer_1", "beer_4", "beer_100");

        launchSearch(filter);
    }

    /**
     * We want to build a Limit Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/limit-filter.html">documentation</a>
     */
    @Test
    public void limitFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.limitFilter(100);

        launchSearch(filter);
    }

    /**
     * We want to build a Type Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/type-filter.html">documentation</a>
     */
    @Test
    public void typeFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.typeFilter("beer");

        launchSearch(filter);
    }

    /**
     * TODO Create a test case
     * We want to build a Geo Bounding Box Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/geo-bounding-box-filter.html">documentation</a>
     */
    @Test @Ignore
    public void geoBoundingBoxFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.geoBoundingBoxFilter("pin.location")
                .topLeft(40.73, -74.1)
                .bottomRight(40.717, -73.99);

        launchSearch(filter);
    }

    /**
     * TODO Create a test case
     * We want to build a Geo Distance Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-filter.html">documentation</a>
     */
    @Test @Ignore
    public void geoDistanceFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.geoDistanceFilter("pin.location")
                .point(40, -70)
                .distance(200, DistanceUnit.KILOMETERS)
                .optimizeBbox("memory")
                .geoDistance(GeoDistance.ARC);

        launchSearch(filter);
    }

    /**
     * TODO Create a test case
     * We want to build a Geo Distance Range Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-range-filter.html">documentation</a>
     */
    @Test @Ignore
    public void geoDistanceRangeFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.geoDistanceRangeFilter("pin.location")
                .point(40, -70)
                .from("200km")
                .to("400km")
                .includeLower(true)
                .includeUpper(false)
                .optimizeBbox("memory")
                .geoDistance(GeoDistance.ARC);

        launchSearch(filter);
    }

    /**
     * TODO Create a test case
     * We want to build a Geo Polygon Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/geo-polygon-filter.html">documentation</a>
     */
    @Test @Ignore
    public void geoPolygonFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.geoPolygonFilter("pin.location")
                .addPoint(40, -70)
                .addPoint(30, -80)
                .addPoint(20, -90);

        launchSearch(filter);
    }

    /**
     * TODO Create a test case
     * We want to build a Geo Shape Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/geo-shape-filter.html">documentation</a>
     */
    @Test @Ignore
    public void geoShapeFilter() throws Exception {
        FilterBuilder filter = null;

        // Shape within another
        filter = FilterBuilders.geoShapeFilter("location",
                new RectangleImpl(0,10,0,10,SpatialContext.GEO))
                .relation(ShapeRelation.WITHIN);

        // Intersect shapes
        filter = FilterBuilders.geoShapeFilter("location",
                new PointImpl(0, 0, SpatialContext.GEO))
                .relation(ShapeRelation.INTERSECTS);

        // Using pre-indexed shapes
        filter = FilterBuilders.geoShapeFilter("location", "New Zealand", "countries")
                .relation(ShapeRelation.DISJOINT);

        launchSearch(filter);
    }

    /**
     * We want to build a Has Child Filter
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/has-child-filter.html">documentation</a>
     */
    @Test
    public void hasChildFilter() throws Exception {
        FilterBuilder qb = FilterBuilders.hasChildFilter("blog_tag",
                QueryBuilders.termQuery("tag", "something"));

        logger.info("Your query is : {}", qb);

        // TODO Create a test here
        // Execute the query
        /*SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());*/
    }

    /**
     * We want to build a Has Parent Filter
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/has-parent-filter.html">documentation</a>
     */
    @Test
    public void hasParentFilter() throws Exception {
        FilterBuilder qb = FilterBuilders.hasParentFilter("blog",
                QueryBuilders.termQuery("tag", "something"));

        logger.info("Your query is : {}", qb);

        // TODO Create a test here
        // Execute the query
        /*SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());*/
    }

    /**
     * We want to build a Match All Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/match-all-filter.html">documentation</a>
     */
    @Test
    public void matchAllFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.matchAllFilter();

        launchSearch(filter);
    }

    /**
     * We want to build a Missing Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/missing-filter.html">documentation</a>
     */
    @Test
    public void missingFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.missingFilter("brand")
                .existence(true)
                .nullValue(true);

        launchSearch(filter);
    }

    /**
     * We want to build a Not Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/not-filter.html">documentation</a>
     */
    @Test
    public void notFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.notFilter(
                FilterBuilders.rangeFilter("price").from("1").to("2")
        );

        launchSearch(filter);
    }

    /**
     * We want to build a Numeric Range Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/numeric-range-filter.html">documentation</a>
     */
    @Test
    public void numericRangeFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.numericRangeFilter("price")
                .from(1)
                .to(2)
                .includeLower(true)
                .includeUpper(false);

        launchSearch(filter);
    }

    /**
     * We want to build a Or Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/or-filter.html">documentation</a>
     */
    @Test
    public void orFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.orFilter(
                FilterBuilders.termFilter("name.second", "banon"),
                FilterBuilders.termFilter("name.nick", "kimchy")
        );

        launchSearch(filter);
    }

    /**
     * We want to build a Prefix Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/prefix-filter.html">documentation</a>
     */
    @Test
    public void prefixFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.prefixFilter("brand", "he");

        launchSearch(filter);
    }

    /**
     * We want to build a Query Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/query-filter.html">documentation</a>
     */
    @Test
    public void queryFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.queryFilter(
                QueryBuilders.queryString("heineken AND pale OR dark")
        );

        launchSearch(filter);
    }


    /**
     * We want to build a Range Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/range-filter.html">documentation</a>
     */
    @Test
    public void rangeFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.rangeFilter("price")
                .from("1")
                .to("2")
                .includeLower(true)
                .includeUpper(false);

        filter = FilterBuilders.rangeFilter("price")
                .gte("1")
                .lt("2");
        launchSearch(filter);
    }

    /**
     * We want to build a Script Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/script-filter.html">documentation</a>
     */
    @Test
    public void scriptFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.scriptFilter(
                "doc['price'].value > param1"
            ).addParam("param1", 5);

        launchSearch(filter);
    }

    /**
     * We want to build a Term Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/term-filter.html">documentation</a>
     */
    @Test
    public void termFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.termFilter("brand", "heineken");

        launchSearch(filter);
    }

    /**
     * We want to build a Terms Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/terms-filter.html">documentation</a>
     */
    @Test
    public void termsFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.termsFilter("brand", "heineken", "kriek")
                .execution("plain"); // "bool", "and" or "or"

        launchSearch(filter);
    }

    /**
     * TODO Create a test
     * We want to build a Nested Filter
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/nested-filter.html">documentation</a>
     */
    @Test @Ignore
    public void nestedFilter() throws Exception {
        FilterBuilder filter = FilterBuilders.nestedFilter("obj1",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("obj1.name", "blue"))
                        .must(QueryBuilders.rangeQuery("obj1.count").gt(5))
        );
        launchSearch(filter);
    }

    private SearchResponse launchSearch(FilterBuilder filter) {
        SearchResponse sr = node.client().prepareSearch()
                .setFilter(filter)
                .execute().actionGet();

        logger.info("Full response is : {}", sr);

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.hits());

        return sr;
    }


}
