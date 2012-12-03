package org.elasticsearchfr.tests;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearchfr.tests.helper.TestNodeHelper;
import org.junit.Assert;
import org.junit.Test;

/**
 * Testing Get Mapping in Java
 * @See https://groups.google.com/forum/?hl=fr&fromgroups=#!topic/elasticsearch/IDZQKxgzR3s
 * @author David Pilato (aka dadoonet)
 */
public class ES002GetMappingTest extends TestNodeHelper {
	protected final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

	/**
	 * @throws Exception
	 */
	@Test
	public void getMapping() throws Exception {
		node.client().prepareIndex("es002index", "type1").setSource("{\"email\" : \"abc@otherdomain.com\", \"firstname\" : \"abc\"}").execute().actionGet();
		node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
		node.client().admin().indices().prepareRefresh().execute().actionGet();

		MappingMetaData md = node.client().admin().cluster().prepareState().execute().actionGet().state().metaData().index("es002index").mapping("type1");

		Assert.assertNotNull(md);
		Assert.assertNotNull(md.source());
		logger.info("Result is : {}", md.source().string());
	}
	
}
