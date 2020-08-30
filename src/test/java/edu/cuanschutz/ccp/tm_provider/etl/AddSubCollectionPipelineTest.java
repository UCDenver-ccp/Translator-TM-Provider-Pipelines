package edu.cuanschutz.ccp.tm_provider.etl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;

import edu.cuanschutz.ccp.tm_provider.etl.util.DatastoreConstants;
import edu.ucdenver.ccp.common.collections.CollectionsUtil;

public class AddSubCollectionPipelineTest {

	@Test
	public void testUpdateStatusEntity() {
		String subCollectionPrefix = "prefix_";

		Entity entity = PipelineTestUtil.createEntity("PMID:1", "PUBMED", "TEST");
		Value origCollectionValues = entity.getPropertiesMap().get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST"),
				getStringValues(DatastoreHelper.getList(origCollectionValues)));

		Entity updatedEntity = AddSubCollectionPipeline.updateStatusEntity(entity, subCollectionPrefix);
		
		assertNotNull(updatedEntity);
		
		Value updatedCollectionValues = updatedEntity.getPropertiesMap()
				.get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		
		assertNotNull(updatedCollectionValues);
		
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST", "prefix_0"),
				getStringValues(DatastoreHelper.getList(updatedCollectionValues)));

	}
	
	@Test
	public void testUpdateStatusEntityEnsureSubCollectionPrefixCorrect() {
		String subCollectionPrefix = "prefix_";

		Entity entity = PipelineTestUtil.createEntity("PMID:1000001", "PUBMED", "TEST");
		Value origCollectionValues = entity.getPropertiesMap().get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST"),
				getStringValues(DatastoreHelper.getList(origCollectionValues)));

		Entity updatedEntity = AddSubCollectionPipeline.updateStatusEntity(entity, subCollectionPrefix);
		
		assertNotNull(updatedEntity);
		
		Value updatedCollectionValues = updatedEntity.getPropertiesMap()
				.get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		
		assertNotNull(updatedCollectionValues);
		
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST", "prefix_1"),
				getStringValues(DatastoreHelper.getList(updatedCollectionValues)));

	}
	
	@Test
	public void testUpdateStatusEntityEnsureSubCollectionPrefixCorrect1() {
		String subCollectionPrefix = "prefix_";

		Entity entity = PipelineTestUtil.createEntity("PMID:1000000", "PUBMED", "TEST");
		Value origCollectionValues = entity.getPropertiesMap().get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST"),
				getStringValues(DatastoreHelper.getList(origCollectionValues)));

		Entity updatedEntity = AddSubCollectionPipeline.updateStatusEntity(entity, subCollectionPrefix);
		
		assertNotNull(updatedEntity);
		
		Value updatedCollectionValues = updatedEntity.getPropertiesMap()
				.get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		
		assertNotNull(updatedCollectionValues);
		
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST", "prefix_1"),
				getStringValues(DatastoreHelper.getList(updatedCollectionValues)));

	}
	
	
	@Test
	public void testUpdateStatusEntityEnsureSubCollectionPrefixCorrect2() {
		String subCollectionPrefix = "prefix_";

		Entity entity = PipelineTestUtil.createEntity("PMID:999999", "PUBMED", "TEST");
		Value origCollectionValues = entity.getPropertiesMap().get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST"),
				getStringValues(DatastoreHelper.getList(origCollectionValues)));

		Entity updatedEntity = AddSubCollectionPipeline.updateStatusEntity(entity, subCollectionPrefix);
		
		assertNotNull(updatedEntity);
		
		Value updatedCollectionValues = updatedEntity.getPropertiesMap()
				.get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		
		assertNotNull(updatedCollectionValues);
		
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST", "prefix_0"),
				getStringValues(DatastoreHelper.getList(updatedCollectionValues)));

	}
	
	@Test
	public void testUpdateStatusEntityEnsureSubCollectionPrefixCorrect3() {
		String subCollectionPrefix = "prefix_";

		Entity entity = PipelineTestUtil.createEntity("PMID:2222222", "PUBMED", "TEST");
		Value origCollectionValues = entity.getPropertiesMap().get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST"),
				getStringValues(DatastoreHelper.getList(origCollectionValues)));

		Entity updatedEntity = AddSubCollectionPipeline.updateStatusEntity(entity, subCollectionPrefix);
		
		assertNotNull(updatedEntity);
		
		Value updatedCollectionValues = updatedEntity.getPropertiesMap()
				.get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		
		assertNotNull(updatedCollectionValues);
		
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST", "prefix_2"),
				getStringValues(DatastoreHelper.getList(updatedCollectionValues)));

	}
	
	
	@Test
	public void testUpdateStatusEntityEnsureOldSubCollectionsAreRemoved() {
		String subCollectionPrefix = "prefix_";

		Entity entity = PipelineTestUtil.createEntity("PMID:1", "PUBMED", "TEST", "prefix_12345");

		Entity updatedEntity = AddSubCollectionPipeline.updateStatusEntity(entity, subCollectionPrefix);
		
		assertNotNull(updatedEntity);
		
		Value updatedCollectionValues = updatedEntity.getPropertiesMap()
				.get(DatastoreConstants.STATUS_PROPERTY_COLLECTIONS);
		
		assertNotNull(updatedCollectionValues);
		
		assertEquals(CollectionsUtil.createSet("PUBMED", "TEST", "prefix_0"),
				getStringValues(DatastoreHelper.getList(updatedCollectionValues)));

	}

	private Set<String> getStringValues(List<Value> values) {
		Set<String> strings = new HashSet<String>();
		for (Value v : values) {
			strings.add(v.getStringValue());
		}
		return strings;
	}

	@Test
	public void testCreateSubCollectionName() {
		String subCollectionPrefix = "prefix_";

		assertEquals("prefix_0", AddSubCollectionPipeline.createSubCollectionName(subCollectionPrefix, "PMID:1"));
		assertEquals("prefix_15",
				AddSubCollectionPipeline.createSubCollectionName(subCollectionPrefix, "PMID:15000001"));
		assertEquals("prefix_32",
				AddSubCollectionPipeline.createSubCollectionName(subCollectionPrefix, "PMID:32840313"));
	}

}
