package org.elasticsearchfr.tests;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearchfr.tests.helper.TestNodeHelper;
import org.junit.Assert;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Testing GeoDistance Filter in Java
 * See discussion on <a href="https://groups.google.com/d/topic/elasticsearch/un-ekdk1qD0/discussion">Elasticsearch Mailing List</a>
 * @author David Pilato (aka dadoonet)
 */
public class ES004GeoDistanceFilterTest extends TestNodeHelper {
	protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

	/**
	 * @throws Exception
	 */
	@Test
	public void putMappingWithNotAnalyzed() throws Exception {
        node.client().admin().indices().prepareCreate("es004index").execute().actionGet();

        XContentBuilder xbMapping = buildMapping();
        logger.info("Mapping is : {}", xbMapping.string());

        PutMappingResponse response = node.client().admin().indices()
            .preparePutMapping("es004index")
            .setType("type1")
            .setSource(xbMapping)
            .execute().actionGet();
        if (!response.isAcknowledged()) {
            throw new Exception("Could not define mapping.");
        }
        node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        node.client().prepareIndex("es004index", "type1").setSource("{\"location\" : { \"lat\" : 5, \"lon\" : 5 }}").execute().actionGet();
		node.client().admin().indices().prepareRefresh().execute().actionGet();

        SearchRequestBuilder srb = node.client().prepareSearch("es004index").setTypes("type1")
                .setQuery(QueryBuilders.matchAllQuery())
                .setFilter(FilterBuilders.geoDistanceFilter("location").distance("0.5km").point(5, 5));
        SearchResponse sr = srb.execute().actionGet();
        Assert.assertNotNull(sr);
        Assert.assertEquals(1L, sr.getHits().totalHits());
    }

    private static XContentBuilder buildMapping() throws Exception {
        return jsonBuilder().prettyPrint()
                .startObject()
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("location").field("type", "geo_point").endObject()
                        .endObject()
                    .endObject()
                .endObject();
    }
}
