package org.elasticsearchfr.tests;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearchfr.tests.helper.TestNodeHelper;
import org.junit.Assert;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Testing Put Mapping with not analyzed field in Java
 * See discussion on <a href="https://groups.google.com/d/topic/elasticsearch/XyvuPxEmBFM/discussion">Elasticsearch Mailing List</a>
 * @author David Pilato (aka dadoonet)
 */
public class ES003PutMappingNotAnalyzedTest extends TestNodeHelper {
	protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

	/**
	 * @throws Exception
	 */
	@Test
	public void putMappingWithNotAnalyzed() throws Exception {
        node.client().admin().indices().prepareCreate("es003index").execute().actionGet();

        XContentBuilder xbMapping = buildMapping();
        logger.info("Mapping is : {}", xbMapping.string());

        PutMappingResponse response = node.client().admin().indices()
            .preparePutMapping("es003index")
            .setType("type1")
            .setSource(xbMapping)
            .execute().actionGet();
        if (!response.acknowledged()) {
            throw new Exception("Could not define mapping.");
        }
        node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        node.client().prepareIndex("es003index", "type1").setSource("{\"analyzed\" : \"Abc Def\", \"notanalyzed\" : \"Abc Def\"}").execute().actionGet();
		node.client().admin().indices().prepareRefresh().execute().actionGet();

        searchTerm("analyzed","abc",1L);
        searchTerm("analyzed","Abc Def",0L);
        searchTerm("notanalyzed","Abc Def",1L);
        searchTerm("notanalyzed", "abc", 0L);
	}

    private void searchTerm(String fieldname, String value, Long expected) {
        SearchResponse sr = node.client().prepareSearch("es003index").setQuery(QueryBuilders.termQuery(fieldname,value)).execute().actionGet();
        Assert.assertNotNull(sr);
        Assert.assertEquals(expected, (Long) sr.getHits().totalHits());
    }

    private static XContentBuilder buildMapping() throws Exception {
        return jsonBuilder().prettyPrint()
                .startObject()
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("analyzed").field("type", "string").field("index", "analyzed").endObject()
                            .startObject("notanalyzed").field("type", "string").field("index", "not_analyzed").endObject()
                        .endObject()
                    .endObject()
                .endObject();
    }

}
