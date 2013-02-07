package org.elasticsearchfr.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.impl.PointImpl;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearchfr.tests.bean.Beer;
import org.elasticsearchfr.tests.bean.BeerHelper;
import org.elasticsearchfr.tests.bean.Colour;
import org.elasticsearchfr.tests.helper.TestNodeHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * We want to have an example for all existing query listed on
 * <a href="http://www.elasticsearch.org/guide/reference/query-dsl/">Query DSL documentation</a>
 * @author David Pilato (aka dadoonet)
 */
public class ES005AllQueriesTest extends TestNodeHelper {
	protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

    /**
     * When we start a test, we index 1000 beers with random data
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        try {
            // Let's delete old index if any
            node.client().admin().indices().prepareDelete("meal").execute().actionGet();
            node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        } catch (ElasticSearchException e) {

        }

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
        try {
            // Let's delete old index if any
            node.client().admin().indices().prepareDelete("meal").execute().actionGet();
            node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        } catch (ElasticSearchException e) {

        }
    }

    /**
     * We want to build a matchAll Query
     * <br>We should have 1000 results
     * <br>We want to display the _source content of the first Hit.
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/match-all-query.html">documentation</a>
     */
    @Test
    public void matchAllSearch() throws Exception {
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        logger.info("Your query is : {}", qb);

        SearchResponse sr = node.client().prepareSearch("meal")
                .setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertEquals(1000, sr.getHits().getTotalHits());

        String jsonFirstHit = sr.getHits().getHits()[0].getSourceAsString();
        logger.info("Your first is : {}", jsonFirstHit);
    }

    /**
     * We want to build a termQuery Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/term-query.html">documentation</a>
     */
    @Test
    public void termSearch() throws Exception {
        QueryBuilder qb = QueryBuilders.termQuery("brand", "heineken");

        logger.info("Your query is : {}", qb);

        SearchResponse sr = node.client().prepareSearch("meal")
                .setQuery(qb)
                .execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a matchQuery Query
     * <br>We should have some results (or we are really unlucky!).
     * <br>Note that we can search "HEINEKEN is a beer"
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/match-query.html">documentation</a>
     */
    @Test
    public void matchSearch() throws Exception {
        QueryBuilder qb = QueryBuilders.matchQuery("brand", "HEINEKEN is a beer");

        logger.info("Your query is : {}", qb);

        SearchResponse sr = node.client().prepareSearch("meal")
                .setQuery(qb)
                .execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a queryString Query
     * <br>We should have some results (or we are really unlucky!).
     * <br>Note that we can search "HEINEKEN is a beer". Note that you can use a Lucene syntax.
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/query-string-query.html">documentation</a>
     * <br>See <a href="http://lucene.apache.org/core/3_6_1/queryparsersyntax.html">documentation</a>
     *
     */
    @Test
    public void queryStringSearch() throws Exception {
        QueryBuilder qb = QueryBuilders.queryString("HEINEKEN");

        logger.info("Your query is : {}", qb);

        SearchResponse sr = node.client().prepareSearch("meal")
                .setQuery(qb).execute()
                .actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a rangeQuery Query on price field to get all beers
     * with price between 5 and 10.
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/range-query.html">documentation</a>
     *
     */
    @Test
    public void rangeSearch() throws Exception {
        QueryBuilder qb = QueryBuilders.rangeQuery("price").from(5).to(10);

        logger.info("Your query is : {}", qb);

        SearchResponse sr = node.client().prepareSearch("meal")
                .setQuery(qb)
                .execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a complex Query based on brand name and price fields.
     * <br>We must find Heineken beers with price between 5 and 10.
     * <br>We should have some results (or we are really unlucky!).
     * <br>We will also get the first 10 hits and convert them into
     * Beer javabeans to check that Elasticsearch has really return
     * what we were looking for.
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/match-query.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/range-query.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/bool-query.html">documentation</a>
     */
    @Test
    public void bool_text_and_range_Search() throws Exception {
        QueryBuilder qb = QueryBuilders
                .boolQuery()
                .must(
                        QueryBuilders.matchQuery("brand", "HEINEKEN")
                )
                .must(
                        QueryBuilders.rangeQuery("price").from(5).to(10)
                );

        logger.info("Your query is : {}", qb);

        SearchResponse sr = node.client().prepareSearch("meal").setQuery(qb)
                .execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());

        for (SearchHit hit : sr.getHits()) {
            Beer beer = BeerHelper.toBeer(hit.getSourceAsString());
            Assert.assertEquals("Heineken", beer.getBrand());
            Assert.assertTrue(beer.getPrice()>5 && beer.getPrice()<10);
        }
    }

    /**
     * We want to build a complex Query based on brand name and price fields and we want to filter results
     * to have only 1L or more beers.
     * <br>We must find Heineken beers with price between 5 and 10 and size more than 1.
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/match-query.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/range-query.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/bool-query.html">documentation</a>
     */
    @Test
    public void query_and_filter_Search() throws Exception {
        QueryBuilder query = QueryBuilders
                .boolQuery()
                .must(
                        QueryBuilders.matchQuery("brand", "HEINEKEN")
                )
                .must(
                        QueryBuilders.rangeQuery("price").from(5).to(10)
                );
        FilterBuilder filter = FilterBuilders.rangeFilter("size").from(1);
        QueryBuilder qb = QueryBuilders.filteredQuery(query, filter);

        logger.info("Your query is : {}", qb);

        SearchResponse sr = node.client().prepareSearch("meal").setQuery(qb)
                .execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());

        for (SearchHit hit : sr.getHits()) {
            Beer beer = BeerHelper.toBeer(hit.getSourceAsString());
            Assert.assertEquals("Heineken", beer.getBrand());
            Assert.assertTrue(beer.getPrice()>5 && beer.getPrice()<10);
            Assert.assertTrue(beer.getSize()>1);
        }

        logger.info("Full json result is: {}", sr.toString());
    }

    /**
     * We want to search like google and see how scoring works on different documents.
     * <br>We will ask for the 100 first documents.
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/query-string-query.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/from-size.html">documentation</a>
     */
    @Test
    public void google_Search() throws Exception {
        QueryBuilder qb = QueryBuilders.queryString("HEINEKEN pale");

        logger.info("Your query is : {}", qb);

        SearchResponse sr = node.client().prepareSearch("meal").setQuery(qb)
                .setSize(100)
                .execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());

        int nbHeineken = 0;
        int nbPale = 0;
        for (SearchHit hit : sr.getHits()) {
            Beer beer = BeerHelper.toBeer(hit.getSourceAsString());

            if ("Heineken".equals(beer.getBrand())) nbHeineken++;
            if (Colour.PALE.equals(beer.getColour())) nbPale++;
        }

        logger.info("For the first 100 beers, we have {} Heineken and {} pale beers", nbHeineken, nbPale);
        logger.info("Full json result is: {}", sr.toString());


    }

    /**
     * We want to search like google and see how scoring works on different documents.
     * <br>We will boost the colour as colour is more important than brand.
     * <br>We will ask for the 100 first documents.
     * <br>We will highlight fields brand and colour
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/query-string-query.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/from-size.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/highlighting.html">documentation</a>
     */
    @Test
    public void google_with_boost_Search() throws Exception {
        QueryBuilder qb = QueryBuilders.queryString("HEINEKEN pale^3");

        logger.info("Your query is : {}", qb);

        SearchResponse sr = node.client().prepareSearch("meal").setQuery(qb)
                .setSize(100)
                .addHighlightedField("brand")
                .addHighlightedField("colour")
                .execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());

        int nbHeineken = 0;
        int nbPale = 0;
        for (SearchHit hit : sr.getHits()) {
            Beer beer = BeerHelper.toBeer(hit.getSourceAsString());

            if ("Heineken".equals(beer.getBrand())) nbHeineken++;
            if (Colour.PALE.equals(beer.getColour())) nbPale++;
        }

        logger.info("For the first 100 beers, we have {} Heineken and {} pale beers", nbHeineken, nbPale);

        // We expect to have more or equals pale beers than heineken
        Assert.assertTrue(nbPale >= nbHeineken);

        logger.info("Full json result is: {}", sr.toString());


    }

    /**
     * We want to use multiSearch API and perform multiple searches in one single call.
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/multi-search.html">documentation</a>
     */
    @Test
    public void multi_Search() throws Exception {
        SearchRequestBuilder srb1 = node.client().prepareSearch("meal").setQuery(QueryBuilders.queryString("pale")).setSize(1);
        logger.info("Your 1st query is : {}", srb1);
        SearchRequestBuilder srb2 = node.client().prepareSearch("meal").setQuery(QueryBuilders.matchQuery("brand", "HEINEKEN")).setSize(1);
        logger.info("Your 2nd query is : {}", srb2);

        MultiSearchResponse sr = node.client().prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertEquals(2, sr.responses().length);
        long nbHits = 0;
        for (MultiSearchResponse.Item item : sr.responses()) {
            SearchResponse response = item.response();
            nbHits += response.hits().totalHits();
        }

        Assert.assertTrue(nbHits > 0);
        logger.info("Full json result is: {}", sr.toString());


    }

    /**
     * We want to build a fuzzyQuery Query
     * <br>We should have some results (or we are really unlucky!).
     * <br>Note that we can search "heinezken"
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/fuzzy-query.html">documentation</a>
     */
    @Test
    public void fuzzySearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.fuzzyQuery("brand", "heinezken");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a MultiMatch Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/multi-match-query.html">documentation</a>
     */
    @Test
    public void multiMatchSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.multiMatchQuery("heineken pale", "brand", "colour");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Boosting Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/boosting-query.html">documentation</a>
     */
    @Test
    public void boostingSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.boostingQuery()
            .positive(QueryBuilders.termQuery("brand","heineken"))
            .negative(QueryBuilders.termQuery("colour","pale"))
            .negativeBoost(0.2f);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a IDs Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/ids-query.html">documentation</a>
     */
    @Test
    public void idsSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.idsQuery().ids("beer_1", "beer_2");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertEquals(sr.getHits().getTotalHits(), 2);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Custom Score Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/custom-score-query.html">documentation</a>
     */
    @Test
    public void customScoreSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query

        qb = QueryBuilders.customScoreQuery(QueryBuilders.matchAllQuery())
                .script("_score * doc['price'].value / pow(param1, param2)")
                .param("param1", 2)
                .param("param2", 3.1);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Custom Boost Factor Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/custom-boost-factor-query.html">documentation</a>
     */
    @Test
    public void customBoostFactorSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.customBoostFactorQuery(QueryBuilders.matchAllQuery()).boostFactor(3.1f);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Constant Score Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/constant-score-query.html">documentation</a>
     */
    @Test
    public void constantScoreSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.constantScoreQuery(FilterBuilders.termFilter("brand", "heineken")).boost(2.0f);
//        qb = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("brand","heineken")).boost(2.0f);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Disjunction Max Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/dis-max-query.html">documentation</a>
     */
    @Test
    public void disMaxSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("brand","heineken"))
                .add(QueryBuilders.termQuery("colour","pale"))
                .boost(1.2f)
                .tieBreaker(0.7f);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Field Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/field-query.html">documentation</a>
     */
    @Test
    public void fieldSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.fieldQuery("brand", "+heineken -grimbergen");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Fuzzy Like This Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/flt-query.html">documentation</a>
     */
    @Test
    public void fuzzyLikeThisSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.fuzzyLikeThisQuery("brand", "color")
                .likeText("heineken is a pale beer")
                .maxQueryTerms(12);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Fuzzy Like This Field Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/flt-field-query.html">documentation</a>
     */
    @Test
    public void fuzzyLikeThisFieldSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.fuzzyLikeThisFieldQuery("brand")
                .likeText("Heineken is a pale beer")
                .maxQueryTerms(12);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Has Child Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/has-child-query.html">documentation</a>
     */
    @Test
    public void hasChildSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.hasChildQuery("blog_tag",
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
     * We want to build a Has Parent Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/has-parent-query.html">documentation</a>
     */
    @Test
    public void hasParentSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.hasParentQuery("blog",
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
     * We want to build a More Like This Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/mlt-query.html">documentation</a>
     */
    @Test
    public void moreLikeThisSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.moreLikeThisQuery("brand", "color")
                .likeText("heineken is a pale beer")
                .minTermFreq(1)
                .maxQueryTerms(12);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a More Like This Field Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/mlt-field-query.html">documentation</a>
     */
    @Test
    public void moreLikeThisFieldSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.moreLikeThisFieldQuery("brand")
                .likeText("Heineken is a pale beer")
                .minTermFreq(1)
                .maxQueryTerms(12);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Prefix Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/mlt-field-query.html">documentation</a>
     */
    @Test
    public void prefixSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.prefixQuery("brand", "heine");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Span First Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/span-first-query.html">documentation</a>
     */
    @Test
    public void spanFirstSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.spanFirstQuery(
                QueryBuilders.spanTermQuery("brand", "heineken"),
                3
        );

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Span Near Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/span-near-query.html">documentation</a>
     */
    @Test
    public void spanNearSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.spanNearQuery()
                .clause(QueryBuilders.spanTermQuery("brand","heineken"))
                .clause(QueryBuilders.spanTermQuery("brand","kriek"))
                .clause(QueryBuilders.spanTermQuery("brand","grimbergen"))
                .slop(12)
                .inOrder(false)
                .collectPayloads(false);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() == 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Span Not Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/span-not-query.html">documentation</a>
     */
    @Test
    public void spanNotSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.spanNotQuery()
                .include(QueryBuilders.spanTermQuery("brand", "heineken"))
                .exclude(QueryBuilders.spanTermQuery("brand", "kriek"));

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Span Or Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/span-or-query.html">documentation</a>
     */
    @Test
    public void spanOrSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.spanOrQuery()
                .clause(QueryBuilders.spanTermQuery("brand", "heineken"))
                .clause(QueryBuilders.spanTermQuery("brand", "grimbergen"))
                .clause(QueryBuilders.spanTermQuery("brand", "kriek"));

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Span Term Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/span-term-query.html">documentation</a>
     */
    @Test
    public void spanTermSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.spanTermQuery("brand","heineken");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Terms Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/terms-query.html">documentation</a>
     */
    @Test
    public void termsSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.termsQuery("brand",
                "heineken", "kriek")
                .minimumMatch(1);

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Top Children Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/top-children-query.html">documentation</a>
     */
    @Test
    public void topChildrenSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.topChildrenQuery("blog_tag",
                QueryBuilders.termQuery("tag", "something"))
                .score("max")
                .factor(5)
                .incrementalFactor(2);

        logger.info("Your query is : {}", qb);

        // TODO Create a test here
        // Execute the query
        /*
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
        */
    }

    /**
     * We want to build a Wildcard Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/wildcard-query.html">documentation</a>
     */
    @Test
    public void wildcardSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.wildcardQuery("brand", "hein?k*");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Nested Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/nested-query.html">documentation</a>
     */
    @Test
    public void nestedSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.nestedQuery("obj1",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("obj1.name", "blue"))
                        .must(QueryBuilders.rangeQuery("obj1.count").gt(5))
            )
            .scoreMode("avg");

        logger.info("Your query is : {}", qb);

        // TODO Create an example
        /*
        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
        */
    }

    /**
     * We want to build a Custom Filters Score Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/custom-filters-score-query.html">documentation</a>
     */
    @Test
    public void customFiltersScoreSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.customFiltersScoreQuery(
                QueryBuilders.matchAllQuery())
            .add(FilterBuilders.rangeFilter("price").from(0).to(3), 3)
            .add(FilterBuilders.rangeFilter("price").from(3).to(20), 2)
            .scoreMode("first");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }


    /**
     * We want to build a Indices Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/indices-query.html">documentation</a>
     */
    @Test
    public void indicesSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.indicesQuery(
                QueryBuilders.termQuery("tag", "wow"),
                "beer", "beer"
            )
            .noMatchQuery(QueryBuilders.termQuery("brand", "heineken"));

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }


    /**
     * We want to build a Indices Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/indices-query.html">documentation</a>
     */
    @Test
    public void indices_all_Search() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.indicesQuery(
                QueryBuilders.termQuery("tag", "wow"),
                "beer", "beer"
        )
                .noMatchQuery("all");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() > 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a Indices Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/indices-query.html">documentation</a>
     */
    @Test
    public void indices_none_Search() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.indicesQuery(
                QueryBuilders.termQuery("tag", "wow"),
                "beer", "beer"
        )
                .noMatchQuery("none");

        logger.info("Your query is : {}", qb);

        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() == 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
    }

    /**
     * We want to build a GeoShape Query
     * <br>We should have some results (or we are really unlucky!).
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/java-api/query-dsl.html">documentation</a>
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/query-dsl/geo-shape-query.html">documentation</a>
     */
    @Test
    public void geoShapeSearch() throws Exception {
        QueryBuilder qb = null;
        // create the query
        qb = QueryBuilders.geoShapeQuery("location",
                new RectangleImpl(0, 10, 0, 10,SpatialContext.GEO))
            .relation(ShapeRelation.WITHIN);
        logger.info("Your query is : {}", qb);

        qb = QueryBuilders.geoShapeQuery("location",
                new PointImpl(0, 0, SpatialContext.GEO))
                .relation(ShapeRelation.INTERSECTS);
        logger.info("Your query is : {}", qb);

        qb = QueryBuilders.geoShapeQuery("location", "New Zealand", "countries")
                .relation(ShapeRelation.DISJOINT);
        logger.info("Your query is : {}", qb);

        // TODO Create examples
        /*
        // Execute the query
        SearchResponse sr = null;
        sr = node.client().prepareSearch("meal").setQuery(qb).execute().actionGet();

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.getHits());
        Assert.assertTrue(sr.getHits().getTotalHits() == 0);

        logger.info("We found {} beers", sr.getHits().totalHits());
        */
    }
}
