package nl.whitelab.neo4j.searchers;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import nl.whitelab.neo4j.DatabasePlugin;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class MetadatumSearcher extends DatabasePlugin {
	protected final GraphDatabaseService database;
	protected final ObjectMapper objectMapper;

	public MetadatumSearcher(@Context GraphDatabaseService database) {
		super(database);
		this.database = database;
		this.objectMapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
				  							  .configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true)
				  							  .configure(SerializationFeature.INDENT_OUTPUT, true);
	}
	
	protected Response getMetadata(final Integer number, final Integer offset, final String sort, final String order) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();
				
				String totalQuery = "MATCH (m:Metadatum) RETURN COUNT(m)";
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(totalQuery) ) {
					Map<String,Object> row = result.next();
					Entry<String, Object> column = row.entrySet().iterator().next();
					jg.writeNumberField("total", (Long) column.getValue());
				}
				
				jg.writeNumberField("number", number);
				jg.writeNumberField("offset", offset);
				jg.writeStringField("sort", sort);
				jg.writeStringField("order", order);
				jg.writeFieldName( "metadata" );
				jg.writeStartArray();
				jg.flush();
				
				String sortString = "m."+sort;
				if (sort.equals("group"))
					sortString = "m.group, m.key";
				
				String orderString = order;
				if (!order.toLowerCase().equals("asc") && !order.toLowerCase().equals("desc"))
					orderString = "asc";
				
				String query = "MATCH (m:Metadatum) RETURN m "
						+ "ORDER BY "+sortString+" "+orderString+" "
						+ "SKIP "+String.valueOf(offset);

				if (number > 0)
					query = query + " LIMIT "+String.valueOf(number);

				try ( Transaction tx = database.beginTx(); Result result = database.execute(query) ) {
					while ( result.hasNext() ) {
						Map<String,Object> row = result.next();
						for ( Entry<String, Object> column : row.entrySet() ) {
							Node metadatum = (Node) column.getValue();
							executor.writeField(metadatum, jg);
						}
						jg.flush();
					}
					tx.success();
				}

				jg.writeEndArray();
				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}
	
	protected Response getMetadataLabel(final String label) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				try ( Transaction tx = database.beginTx() ) {
					JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
					IndexManager index = database.index();
					Index<Node> metadata = index.forNodes("Metadatum");
					jg.writeStartArray();
					for ( Node metadatum : metadata.query( "label:"+label ) ) {
						executor.writeField(metadatum, jg);
					}
					jg.writeEndArray();
					jg.flush();
					tx.success();
				}
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}
	
	protected Response updateMetadataLabel(final String label, final String property, final String value) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				try ( Transaction tx = database.beginTx() ) {
					JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
					IndexManager index = database.index();
					Index<Node> metadata = index.forNodes("Metadatum");
					for ( Node metadatum : metadata.query( "label:"+label ) ) {
						if (property.equals("explorable") || property.equals("searchable"))
							metadatum.setProperty(property, Boolean.valueOf(value));
						else
							metadatum.setProperty(property, value);
					}
					jg.writeString("Updated "+label);
					jg.flush();
					tx.success();
				}
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}
	
	protected Response getMetadataGroupKey(final String group, final String key) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				try ( Transaction tx = database.beginTx() ) {
					JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
					IndexManager index = database.index();
					Index<Node> metadata = index.forNodes("Metadatum");
					jg.writeStartArray();
					for ( Node metadatum : metadata.query( "group:"+group+" AND key:"+key ) ) {
						executor.writeField(metadatum, jg);
					}
					jg.writeEndArray();
					jg.flush();
					tx.success();
				}
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	protected Response getMetadataGroupKeyValues(final String group, final String key, final Integer number, final Integer offset, final String sort, final String order) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();

				if (group == null || group.isEmpty() || key == null || key.isEmpty()) {
					jg.writeStringField("error", "No label supplied.");
				} else {
					jg.writeStringField("group", group);
					jg.writeStringField("key", key);
					jg.writeNumberField("number", number);
					jg.writeNumberField("offset", offset);
					
					String sortString = "value";
					String orderString = "asc";
	
					jg.writeStringField("sort", sortString);
					jg.writeStringField("order", orderString);
					
					String query = "";
					
					if (group.equals("Corpus") || group.equals("Collection")) {
						query = "MATCH (c:"+group+") RETURN DISTINCT c."+key+" AS value ";
					} else {
						query = "START m=node:Metadatum('group:"+group+" AND key:"+key+"') "
							+ "MATCH (m)<-[v:HAS_METADATUM]-(:Document) "
							+ "WITH DISTINCT CASE WHEN v.value = true THEN 'true' ELSE CASE WHEN v.value = false THEN 'false' ELSE TOSTRING(v.value) END END AS value "
							+ "RETURN value ";
					}
					
					query = query + "ORDER BY "+sortString+" "+orderString+" SKIP "+String.valueOf(offset);
	
					if (number > 0)
						query = query + " LIMIT "+String.valueOf(number);
					
					System.out.println("CYPHER: "+query);

					jg.writeStringField("cypher", query);
					jg.writeFieldName( "values" );
					jg.writeStartArray();
					try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
						while ( result.hasNext() ) {
							jg.writeStartObject();
							Map<String,Object> row = result.next();
							for ( Entry<String, Object> column : row.entrySet() ) {
								executor.writeField(column, jg);
							}
							jg.writeEndObject();
						}
					}
					jg.writeEndArray();
				}

				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	protected Response getMetadataGroupKeyValuesWithCounts(final String group, final String key, final Integer number, final Integer offset, final String sort, final String order) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();

				if (group == null || group.isEmpty() || key == null || key.isEmpty()) {
					jg.writeStringField("error", "No group or key supplied.");
				} else {
					jg.writeStringField("group", group);
					jg.writeStringField("key", key);
					jg.writeNumberField("number", number);
					jg.writeNumberField("offset", offset);
					
					String sortString = sort.toLowerCase();
					if (!sortString.equals("document_count") && !sortString.equals("value"))
						sortString = "document_count";
//					if (sortString.equals("value"))
//						sortString = "TOSTRING(value)";
					
					String orderString = order;
					if (!order.toLowerCase().equals("asc") && !order.toLowerCase().equals("desc"))
						orderString = "asc";
	
					jg.writeStringField("sort", sortString);
					jg.writeStringField("order", orderString);
					
					String query = "";
					
					if (group.equals("Corpus")) {
						query = "MATCH (corpus:Corpus)-[:HAS_COLLECTION]->(:Collection)-[:HAS_DOCUMENT]->(:Document) "
								+ "WHERE 'title' IN keys(corpus) "
								+ "WITH DISTINCT corpus "
								+ "MATCH (corpus)-[:HAS_COLLECTION]->(:Collection)-[:HAS_DOCUMENT]->(d:Document) "
								+ "WITH DISTINCT corpus.title AS value, COUNT(DISTINCT d.xmlid) AS document_count "
								+ "RETURN value, document_count ";
					} else if (group.equals("Collection")) {
						query = "MATCH (collection:Collection)-[:HAS_DOCUMENT]->(:Document) "
								+ "WHERE '"+key+"' IN keys(collection) "
								+ "WITH DISTINCT collection "
								+ "MATCH (c:Corpus) "
								+ "WITH DISTINCT c.label AS corpus_label, collection "
								+ "MATCH (collection)-[:HAS_DOCUMENT]->(d:Document) "
								+ "OPTIONAL MATCH (collection)-[:HAS_DOCUMENT]->(d2:Document{corpus_label:corpus_label}) "
								+ "WITH DISTINCT collection."+key+" AS value, corpus_label AS corpus, COUNT(DISTINCT d.xmlid) AS document_count, CASE WHEN COUNT(*) > 0 THEN COUNT(DISTINCT d2) ELSE 0 END AS corpus_document_count "
								+ "RETURN value, document_count, { corpora: COLLECT(corpus), counts: COLLECT(corpus_document_count) } AS corpus_counts ";
					} else {
						query = "START m=node:Metadatum('group:"+group+" AND key:"+key+"') "
								+ "MATCH (m)<-[v:HAS_METADATUM]-(:Document) "
								+ "WITH DISTINCT v, m "
								+ "MATCH (c:Corpus) "
								+ "WITH DISTINCT c.label AS corpus_label, v, m "
								+ "MATCH (m)<-[v]-(d:Document) "
								+ "OPTIONAL MATCH (m)<-[v]-(d2:Document{corpus_label:corpus_label}) "
								+ "WITH DISTINCT CASE WHEN v.value = true THEN 'true' ELSE CASE WHEN v.value = false THEN 'false' ELSE TOSTRING(v.value) END END AS value, corpus_label AS corpus, COUNT(DISTINCT d.xmlid) AS document_count, "
								+ "CASE WHEN COUNT(*) > 0 THEN COUNT(DISTINCT d2) ELSE 0 END AS corpus_document_count "
								+ "RETURN value, document_count, { corpora: COLLECT(corpus), counts: COLLECT(corpus_document_count) } AS corpus_counts ";
					}
					
					query = query + "ORDER BY "+sortString+" "+orderString+" SKIP "+String.valueOf(offset);
	
					if (number > 0)
						query = query + " LIMIT "+String.valueOf(number);
					
					System.out.println("CYPHER: "+query);

					jg.writeStringField("cypher", query);
					jg.writeFieldName( "values" );
					jg.writeStartArray();
					try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
						while ( result.hasNext() ) {
							jg.writeStartObject();
							Map<String,Object> row = result.next();
							for ( Entry<String, Object> column : row.entrySet() ) {
								executor.writeField(column, jg);
							}
							jg.writeEndObject();
						}
					}
					jg.writeEndArray();
				}

				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	protected Response getMetadataLabelValues(final String label, final Integer number, final Integer offset, final String sort, final String order) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();

				if (label == null || label.isEmpty()) {
					jg.writeStringField("error", "No label supplied.");
				} else {
					jg.writeStringField("label", label);
					jg.writeNumberField("number", number);
					jg.writeNumberField("offset", offset);
					
					String sortString = sort.toLowerCase();
					if (!sortString.equals("document_count") && !sortString.equals("value"))
						sortString = "document_count";
//					if (sortString.equals("value"))
//						sortString = "TOSTRING(value)";
					
					String orderString = order;
					if (!order.toLowerCase().equals("asc") && !order.toLowerCase().equals("desc"))
						orderString = "asc";
	
					jg.writeStringField("sort", sortString);
					jg.writeStringField("order", orderString);
					
					String query = "";
					
					query = "START m=node:Metadatum('label:"+label+"') "
						+ "MATCH (m)<-[v:HAS_METADATUM]-(:Document) "
						+ "WITH DISTINCT CASE WHEN v.value = true THEN 'true' ELSE CASE WHEN v.value = false THEN 'false' ELSE TOSTRING(v.value) END END AS value "
						+ "RETURN value ";
					
					
					query = query + "ORDER BY "+sortString+" "+orderString+" SKIP "+String.valueOf(offset);
	
					if (number > 0)
						query = query + " LIMIT "+String.valueOf(number);
					
					System.out.println("CYPHER: "+query);

					jg.writeStringField("cypher", query);
					jg.writeFieldName( "values" );
					jg.writeStartArray();
					try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
						while ( result.hasNext() ) {
							jg.writeStartObject();
							Map<String,Object> row = result.next();
							for ( Entry<String, Object> column : row.entrySet() ) {
								executor.writeField(column, jg);
							}
							jg.writeEndObject();
						}
					}
					jg.writeEndArray();
				}

				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	protected Response getMetadataLabelValuesWithCounts(final String label, final Integer number, final Integer offset, final String sort, final String order) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();

				if (label == null || label.isEmpty()) {
					jg.writeStringField("error", "No label supplied.");
				} else {
					jg.writeStringField("label", label);
					jg.writeNumberField("number", number);
					jg.writeNumberField("offset", offset);
					
					String sortString = sort.toLowerCase();
					if (!sortString.equals("document_count") && !sortString.equals("value"))
						sortString = "document_count";
//					if (sortString.equals("value"))
//						sortString = "TOSTRING(value)";
					
					String orderString = order;
					if (!order.toLowerCase().equals("asc") && !order.toLowerCase().equals("desc"))
						orderString = "asc";
	
					jg.writeStringField("sort", sortString);
					jg.writeStringField("order", orderString);
					
					String query = "";
					
					query = "START m=node:Metadatum('label:"+label+"') "
						+ "MATCH (m)<-[v:HAS_METADATUM]-(:Document) "
						+ "WITH DISTINCT v, m "
						+ "MATCH (c:Corpus) "
						+ "WITH DISTINCT c.label AS corpus_label, v, m "
						+ "MATCH (m)<-[v]-(d:Document) "
						+ "OPTIONAL MATCH (m)<-[v]-(d2:Document{corpus_label:corpus_label}) "
						+ "WITH DISTINCT CASE WHEN v.value = true THEN 'true' ELSE CASE WHEN v.value = false THEN 'false' ELSE TOSTRING(v.value) END END AS value, corpus_label AS corpus, COUNT(DISTINCT d.xmlid) AS document_count, "
						+ "CASE WHEN COUNT(*) > 0 THEN COUNT(DISTINCT d2) ELSE 0 END AS corpus_document_count "
						+ "RETURN value, document_count, { corpora: COLLECT(corpus), counts: COLLECT(corpus_document_count) } AS corpus_counts ";
					
					
					query = query + "ORDER BY "+sortString+" "+orderString+" SKIP "+String.valueOf(offset);
	
					if (number > 0)
						query = query + " LIMIT "+String.valueOf(number);
					
					System.out.println("CYPHER: "+query);

					jg.writeStringField("cypher", query);
					jg.writeFieldName( "values" );
					jg.writeStartArray();
					try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
						while ( result.hasNext() ) {
							jg.writeStartObject();
							Map<String,Object> row = result.next();
							for ( Entry<String, Object> column : row.entrySet() ) {
								executor.writeField(column, jg);
							}
							jg.writeEndObject();
						}
					}
					jg.writeEndArray();
				}

				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	protected Response getMetadataGroupKeyValueDocuments(final String group, final String key, final String value) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				try ( Transaction tx = database.beginTx() ) {
					JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
					IndexManager index = database.index();
					Index<Relationship> metadataValues = index.forRelationships("HAS_METADATUM");
					List<String> documentsSeen = new ArrayList<String>();
					jg.writeStartArray();
					
					System.out.println("VALUE: "+value);
					for (Relationship hasMetadatum : metadataValues.get("value", value)) {
						Node metadatum = hasMetadatum.getEndNode();
						String mGroup = (String) metadatum.getProperty("group");
						String mKey = (String) metadatum.getProperty("key");
						if (mGroup.equals(group) && mKey.equals(key)) {
							Node document = hasMetadatum.getStartNode();
							String xmlid = (String) document.getProperty("xmlid");
							if (!documentsSeen.contains(xmlid)) {
								jg.writeString(xmlid);
								documentsSeen.add(xmlid);
							}
						}
					}
					
					jg.writeEndArray();
					jg.flush();
					tx.success();
				}
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	protected Response getMetadataGroupKeyValueDocumentsWithCounts(final String group, final String key, final String value) {
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				try ( Transaction tx = database.beginTx() ) {
					JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
					IndexManager index = database.index();
					Index<Relationship> metadataValues = index.forRelationships("HAS_METADATUM");
					List<String> documentsSeen = new ArrayList<String>();
					jg.writeStartArray();
					
					System.out.println("VALUE: "+value);
					for (Relationship hasMetadatum : metadataValues.get("value", value)) {
						Node metadatum = hasMetadatum.getEndNode();
						String mGroup = (String) metadatum.getProperty("group");
						String mKey = (String) metadatum.getProperty("key");
						if (mGroup.equals(group) && mKey.equals(key)) {
							Node document = hasMetadatum.getStartNode();
							String xmlid = (String) document.getProperty("xmlid");
							if (!documentsSeen.contains(xmlid)) {
								jg.writeStartObject();
								jg.writeStringField("xmlid", xmlid);
								Object tokenCount = document.getProperty("token_count");
								if (tokenCount instanceof Long)
									jg.writeNumberField("token_count", (Long) tokenCount);
								else if (tokenCount instanceof Integer)
									jg.writeNumberField("token_count", (Integer) tokenCount);
								jg.writeEndObject();
								documentsSeen.add(xmlid);
							}
						}
					}
					
					jg.writeEndArray();
					jg.flush();
					tx.success();
				}
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

//	protected Response getMetadataGroupKeyAverageDocumentSizeVsTotalSize(final String group, final String key, final String filter) {
//		StreamingOutput stream = new StreamingOutput()
//		{
//			@Override
//			public void write( OutputStream os ) throws IOException, WebApplicationException
//			{
//				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
//				jg.writeStartObject();
//
//				if (group.isEmpty() || key.isEmpty()) {
//					jg.writeStringField("error", "Insufficient parameters (required: group, key, filter).");
//				} else {
//					jg.writeStringField("group", group);
//					jg.writeStringField("key", key);
//					
//					String query = "";
//
//					if (!filter.isEmpty()) {
//						jg.writeStringField("filter", filter);
//					} else {
//						if (group.equals("Corpus")) {
//							query = "MATCH (corpus:Corpus) "
//									+ "RETURN corpus.title AS title, corpus.token_count AS total_size, corpus.token_count / corpus.document_count AS avg_document_size, corpus.document_count AS document_count;";
//						} else if (group.equals("Collection")) {
//							query = "MATCH (collection:Collection) "
//									+ "RETURN collection.title AS title, collection.token_count AS total_size, collection.token_count / collection.document_count AS avg_document_size, collection.document_count AS document_count;";
//						} else {
//							query = "START metadatum=node:Metadatum('group:"+group+" AND key:"+key+"') "
//									+ "MATCH metadatum-[r:HAS_METADATUM]-(document:Document) "
//									+ "WITH DISTINCT r.value AS title, COUNT(DISTINCT document) AS document_count, SUM(document.token_count) AS total_size "
//									+ "RETURN title, total_size, total_size / document_count AS avg_document_size, document_count "
//									+ "UNION "
//									+ "START metadatum=node:Metadatum('group:"+group+" AND key:"+key+"') "
//									+ "MATCH (document:Document) "
//									+ "WHERE NOT document-[:HAS_METADATUM]->metadatum "
//									+ "WITH 'Unknown' AS title, SUM(document.token_count) AS total_size, COUNT(DISTINCT document) AS document_count "
//									+ "RETURN title, total_size, total_size / document_count AS avg_document_size, document_count;";
//						}
//					}
//					
//					System.out.println("CYPHER: "+query);
//
//					jg.writeStringField("cypher", query);
//					jg.writeFieldName( "results" );
//					jg.writeStartArray();
//					try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
//						while ( result.hasNext() ) {
//							jg.writeStartObject();
//							Map<String,Object> row = result.next();
//							for ( Entry<String, Object> column : row.entrySet() ) {
//								executor.writeField(column, jg);
//							}
//							jg.writeEndObject();
//						}
//					}
//					jg.writeEndArray();
//				}
//
//				jg.writeEndObject();
//				jg.flush();
//				jg.close();
//			}
//		};
//
//		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
//	}

}
