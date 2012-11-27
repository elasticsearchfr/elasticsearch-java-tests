package org.elasticsearchfr.tests;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearchfr.tests.helper.TestNodeHelper;
import org.junit.Assert;
import org.junit.Test;

/**
 * Testing bool queries with must clauses
 * @See https://groups.google.com/forum/?hl=fr&fromgroups=#!topic/elasticsearch/2nHYMryvOcE
 * @author David Pilato (aka dadoonet)
 */
public class ES1Test extends TestNodeHelper {
	protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

	/**
	 * @throws Exception
	 */
	@Test
	public void createSuggest() throws Exception {
		node.client().prepareIndex("es1index", "type1").setSource("{\"email\" : \"abc@otherdomain.com\", \"firstname\" : \"abc\"}").execute().actionGet();
		node.client().prepareIndex("es1index", "type1").setSource("{\"email\" : \"abc@gmail.com\", \"firstname\" : \"abc\"}").execute().actionGet();
		node.client().prepareIndex("es1index", "type1").setSource("{\"email\" : \"xyz@gmail.com\", \"firstname\" : \"xyz\"}").execute().actionGet();
		
		node.client().admin().indices().prepareRefresh().execute().actionGet();

		QueryBuilder qb = QueryBuilders
				.boolQuery()
					.must(QueryBuilders.wildcardQuery("email", "*gmail.com*"))
					.must(QueryBuilders.termQuery("firstname","abc"));

		logger.info("Your query is : {}", qb);

		SearchResponse sr = node.client().prepareSearch().setQuery(qb)
				.execute().actionGet();

		logger.info("Result is {}", sr.toString());

		Assert.assertNotNull(sr);
		Assert.assertNotNull(sr.getHits());
		Assert.assertEquals(1, sr.getHits().getTotalHits());
	}
	
}
