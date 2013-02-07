package org.elasticsearchfr.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.AbstractFacetBuilder;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacetBuilder;
import org.elasticsearch.search.facet.filter.FilterFacet;
import org.elasticsearch.search.facet.filter.FilterFacetBuilder;
import org.elasticsearch.search.facet.geodistance.GeoDistanceFacet;
import org.elasticsearch.search.facet.geodistance.GeoDistanceFacetBuilder;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacetBuilder;
import org.elasticsearch.search.facet.query.QueryFacet;
import org.elasticsearch.search.facet.query.QueryFacetBuilder;
import org.elasticsearch.search.facet.range.RangeFacet;
import org.elasticsearch.search.facet.range.RangeFacetBuilder;
import org.elasticsearch.search.facet.statistical.StatisticalFacet;
import org.elasticsearch.search.facet.statistical.StatisticalFacetBuilder;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacetBuilder;
import org.elasticsearchfr.tests.bean.Beer;
import org.elasticsearchfr.tests.bean.BeerHelper;
import org.elasticsearchfr.tests.helper.TestNodeHelper;
import org.junit.*;

/**
 * We want to have an example for all existing facet listed on
 * <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/">Facets documentation</a>
 * @author David Pilato (aka dadoonet)
 */
public class ES007AllFacetsTest extends TestNodeHelper {
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
     * We want to build a termsFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/terms-facet.html">documentation</a>
     */
    @Test
    public void termsFacet() throws Exception {
        TermsFacetBuilder facet = FacetBuilders.termsFacet("f")
                .field("brand")
                .size(10);

        TermsFacet f = (TermsFacet) launchSearch(facet, "f");

        f.getTotalCount();      // Total terms doc count
        f.getOtherCount();      // Not shown terms doc count
        f.getMissingCount();    // Without term doc count

        // For each entry
        for (TermsFacet.Entry entry : f) {
            entry.getTerm();    // Term
            entry.getCount();   // Doc count
        }

    }

    /**
     * We want to build a rangeFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/range-facet.html">documentation</a>
     */
    @Test
    public void rangeFacet() throws Exception {
        RangeFacetBuilder facet = FacetBuilders.rangeFacet("f")
                .field("price")
                .addUnboundedFrom(3)
                .addRange(3, 6)
                .addUnboundedTo(6);

        RangeFacet f = (RangeFacet) launchSearch(facet, "f");

        // For each entry
        for (RangeFacet.Entry entry : f) {
            entry.getFrom();    // Range from requested
            entry.getTo();      // Range to requested
            entry.getCount();   // Doc count
            entry.getMin();     // Min value
            entry.getMax();     // Max value
            entry.getMean();    // Mean
            entry.getTotal();   // Sum of values
        }

    }

    /**
     * We want to build a histogramFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/histogram-facet.html">documentation</a>
     */
    @Test
    public void histogramFacet() throws Exception {
        HistogramFacetBuilder facet = FacetBuilders.histogramFacet("f")
                .field("price")
                .interval(1);

        HistogramFacet f = (HistogramFacet) launchSearch(facet, "f");

        // For each entry
        for (HistogramFacet.Entry entry : f) {
            entry.getKey();     // Key (X-Axis)
            entry.getCount();   // Doc count (Y-Axis)
        }

    }

    /**
     * We want to build a dateHistogramFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/date-histogram-facet.html">documentation</a>
     */
    @Test
    public void dateHistogramFacet() throws Exception {
        DateHistogramFacetBuilder facet = FacetBuilders.dateHistogramFacet("f")
                .field("date")
                .interval("year");

        DateHistogramFacet f = (DateHistogramFacet) launchSearch(facet, "f");

        // For each entry
        for (DateHistogramFacet.Entry entry : f) {
            entry.getTime();    // Date in ms since epoch (X-Axis)
            entry.getCount();   // Doc count (Y-Axis)
        }
    }

    /**
     * We want to build a filterFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/filter-facet.html">documentation</a>
     */
    @Test
    public void filterFacet() throws Exception {
        FilterFacetBuilder facet = FacetBuilders.filterFacet("f",
                FilterBuilders.termFilter("brand", "heineken"));

        FilterFacet f = (FilterFacet) launchSearch(facet, "f");

        f.getCount();   // Number of docs that matched
    }


    /**
     * We want to build a queryFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/query-facet.html">documentation</a>
     */
    @Test
    public void queryFacet() throws Exception {
        QueryFacetBuilder facet = FacetBuilders.queryFacet("f",
                QueryBuilders.matchQuery("brand", "heineken"));

        QueryFacet f = (QueryFacet) launchSearch(facet, "f");

        f.getCount();   // Number of docs that matched
    }

    /**
     * We want to build a statisticalFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/statistical-facet.html">documentation</a>
     */
    @Test
    public void statisticalFacet() throws Exception {
        StatisticalFacetBuilder facet = FacetBuilders.statisticalFacet("f")
               .field("price");

        StatisticalFacet f = (StatisticalFacet) launchSearch(facet, "f");

        f.getCount();           // Doc count
        f.getMin();             // Min value
        f.getMax();             // Max value
        f.getMean();            // Mean
        f.getTotal();           // Sum of values
        f.getStdDeviation();    // Standard Deviation
        f.getSumOfSquares();    // Sum of Squares
        f.getVariance();        // Variance
    }

    /**
     * We want to build a statisticalFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/terms-stats-facet.html">documentation</a>
     */
    @Test
    public void termsStatsFacet() throws Exception {
        TermsStatsFacetBuilder facet = FacetBuilders.termsStatsFacet("f")
                .keyField("brand")
                .valueField("price");

        TermsStatsFacet f = (TermsStatsFacet) launchSearch(facet, "f");

        // For each entry
        for (TermsStatsFacet.Entry entry : f) {
            entry.getTerm();            // Term
            entry.getCount();           // Doc count
            entry.getMin();             // Min value
            entry.getMax();             // Max value
            entry.getMean();            // Mean
            entry.getTotal();           // Sum of values
        }
    }

    /**
     * We want to build a geoDistanceFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/geo-distance-facet.html">documentation</a>
     * TODO Create real test
     */
    @Test @Ignore
    public void geoDistanceFacet() throws Exception {
        GeoDistanceFacetBuilder facet = FacetBuilders.geoDistanceFacet("f")
                .field("location")
                .point(40, -70)
                .addUnboundedFrom(10)
                .addRange(10, 20)
                .addRange(20, 100)
                .addUnboundedTo(100)
                .unit(DistanceUnit.KILOMETERS)
                ;

        GeoDistanceFacet f = (GeoDistanceFacet) launchSearch(facet, "f");

        // For each entry
        for (GeoDistanceFacet.Entry entry : f) {
            entry.getFrom();            // Distance from requested
            entry.getTo();              // Distance to requested
            entry.getCount();           // Doc count
            entry.getMin();             // Min value
            entry.getMax();             // Max value
            entry.getTotal();           // Sum of values
            entry.getMean();            // Mean
        }
    }

    /**
     * We want to build a termsFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/terms-facet.html">documentation</a>
     */
    @Test
    public void facetFilterOnTermFacet() throws Exception {
        TermsFacetBuilder facet = FacetBuilders.termsFacet("f")
                .field("brand")
                .facetFilter(FilterBuilders.termFilter("colour", "pale"));

        TermsFacet f = (TermsFacet) launchSearch(facet, "f");

        f.getTotalCount();      // Total terms doc count
        f.getOtherCount();      // Not shown terms doc count
        f.getMissingCount();    // Without term doc count

        // For each entry
        for (TermsFacet.Entry entry : f) {
            entry.getTerm();    // Term
            entry.getCount();   // Doc count
        }

    }

    /**
     * We want to build a termsFacet
     * @throws Exception
     * <br>See <a href="http://www.elasticsearch.org/guide/reference/api/search/facets/terms-facet.html">documentation</a>
     */
    @Test
    public void sameFilterOnTermFacet() throws Exception {
        FilterBuilder filter = FilterBuilders.termFilter("colour", "pale");

        TermsFacetBuilder facet = FacetBuilders.termsFacet("f")
                .field("brand")
                .facetFilter(filter);

        SearchResponse sr = node.client().prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .setFilter(filter)
                .addFacet(facet)
                .execute().actionGet();

        logger.info("Full response is : {}", sr);

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.facets());
        Assert.assertNotNull(sr.facets().facetsAsMap().get("f"));
        Assert.assertEquals("f", sr.facets().facetsAsMap().get("f").getName());


        TermsFacet f = (TermsFacet) sr.facets().facetsAsMap().get("f");

        f.getTotalCount();      // Total terms doc count
        f.getOtherCount();      // Not shown terms doc count
        f.getMissingCount();    // Without term doc count

        // For each entry
        for (TermsFacet.Entry entry : f) {
            entry.getTerm();    // Term
            entry.getCount();   // Doc count
        }

    }

    private Facet launchSearch(AbstractFacetBuilder facet, String facetName) {
        SearchResponse sr = node.client().prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .addFacet(facet)
                .execute().actionGet();

        logger.info("Full response is : {}", sr);

        Assert.assertNotNull(sr);
        Assert.assertNotNull(sr.facets());
        Assert.assertNotNull(sr.facets().facetsAsMap().get(facetName));
        Assert.assertEquals(facetName, sr.facets().facetsAsMap().get(facetName).getName());

        return sr.facets().facetsAsMap().get(facetName);
    }


}
