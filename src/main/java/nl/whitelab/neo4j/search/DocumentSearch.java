package nl.whitelab.neo4j.search;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import nl.whitelab.neo4j.DatabasePlugin;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@Path("/docs")
public class DocumentSearch extends DatabasePlugin {
	protected final ObjectMapper objectMapper;

	public DocumentSearch(@Context GraphDatabaseService database) {
		super(database);
		this.objectMapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
				  							  .configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true)
				  							  .configure(SerializationFeature.INDENT_OUTPUT, true);
	}
	
	@POST
	public Response doPost
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId )
	{
		System.out.println("DocumentSearcher.doPost");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, null, null, null, null, null, 2, number, offset, 5, false));
	}

	@GET
	public Response doGet
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId )
	{
		System.out.println("DocumentSearcher.doGet");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, null, null, null, null, null, 2, number, offset, 5, false));
	}

	@GET
	@Path( "/count" )
	public Response doGetCount
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("5000") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId )
	{
		System.out.println("DocumentSearcher.doGetCount");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, null, null, null, null, null, 2, number, offset, 0, true));
	}

	@POST
	@Path( "/count" )
	public Response doPostCount
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("5000") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId )
	{
		System.out.println("DocumentSearcher.doPostCount");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, null, null, null, null, null, 2, number, offset, 0, true));
	}
	
	@GET
	@Path( "/{xmlid}/content" )
	public Response getDocumentContent
			( @PathParam("xmlid") final String xmlid, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset,
			  @QueryParam("pattern") final String cqlPattern,
			  @DefaultValue("document") @QueryParam("pattern") final String within ) {
		System.out.println("DocumentSearcher.doPostCount");
		return getQueryResult(constructor.parseQuery(null, within, null, cqlPattern, null, null, null, null, null, xmlid, 24, number, offset, 0, false));
	}
	
//	@GET
//	@Path( "/{xmlid}/content" )
//	public Response getDocumentContentOld
//			( @PathParam("xmlid") final String xmlid, 
//			  @DefaultValue("50") @QueryParam("number") final Integer number, 
//			  @DefaultValue("0") @QueryParam("offset") final Integer offset ) {
//		StreamingOutput stream = new StreamingOutput()
//		{
//			@Override
//			public void write( OutputStream os ) throws IOException, WebApplicationException
//			{
//				final JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
//				jg.writeStartObject();
//				jg.writeStringField("document_xmlid", xmlid);
//				
//				String query = "START document=node:Document('xmlid:"+xmlid+"') "
//						+ "MATCH (s:Sentence)-[:OCCURS_IN]->document "
//						+ "WITH document, COUNT(DISTINCT s) AS total_sentence_count "
//						+ "MATCH (s:Sentence)-[:OCCURS_IN]->document "
//						+ "WITH DISTINCT document, total_sentence_count, s "
//						+ "ORDER BY s.start_index "
//						+ "WITH document, total_sentence_count, s "
//						+ "SKIP "+String.valueOf(offset)+" ";
//				
//				if (number > 0)
//					query = query + "LIMIT "+String.valueOf(number)+" ";
//				
//				query = query + "WITH document, MIN(s.start_index) AS min_index, MAX(s.end_index) AS max_index, total_sentence_count "
//						+ "MATCH document-[:HAS_TOKEN]->(t:WordToken) "
//						+ "WHERE t.token_index >= min_index AND t.token_index <= max_index "
//						+ "WITH document.mp3 AS audio_file, total_sentence_count, COLLECT(t) AS tokens "
//						+ "UNWIND tokens AS t "
//						+ "MATCH t-[:HAS_TYPE]->(w:WordType) "
//						+ "MATCH t-[:HAS_LEMMA]->(l:Lemma) "
//						+ "MATCH t-[:HAS_POS_TAG]->(p:PosTag) "
//						+ "MATCH t-[:HAS_PHONETIC]->(ph:Phonetic) "
//						+ "WITH audio_file, total_sentence_count, t, w.label AS word_type, l.label AS lemma, p.label AS pos_tag, ph.label AS phonetic "
//						+ "OPTIONAL MATCH t<-[r:STARTS_AT]-(s:Sentence) "
//						+ "WITH audio_file, total_sentence_count, t, word_type, lemma, pos_tag, phonetic, CASE WHEN s IS NOT NULL THEN 'true' ELSE 'false' END AS sentence_start, CASE WHEN r.actor_code IS NOT NULL THEN r.actor_code ELSE '' END AS speaker "
//						+ "OPTIONAL MATCH t<-[:STARTS_AT]-(p:ParagraphStart) "
//						+ "WITH audio_file, total_sentence_count, t, word_type, lemma, pos_tag, phonetic, sentence_start, speaker, CASE WHEN p IS NOT NULL THEN 'true' ELSE 'false' END AS paragraph_start "
//						+ "ORDER BY t.token_index "
//						+ "RETURN audio_file, total_sentence_count, COLLECT({ token_index: t.token_index, xmlid: t.xmlid, begin_time: t.begin_time, end_time: t.end_time, word_type: word_type, lemma: lemma, pos_tag: pos_tag, phonetic: phonetic, sentence_speaker: speaker, sentence_start: sentence_start, paragraph_start: paragraph_start }) AS content;";
//				
//				System.out.println(query);
//				
//				Long start = System.currentTimeMillis();
//				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
//					Long end = System.currentTimeMillis();
//					Long duration = end - start;
//					jg.writeNumberField("duration", duration);
//					while ( result.hasNext() ) {
//						Map<String,Object> row = result.next();
//						for ( Entry<String, Object> column : row.entrySet() ) {
//							executor.writeField(column, jg);
//						}
//					}
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

	@GET
	@Path( "/{xmlid}/metadata" )
	public Response getDocumentMetadata( @PathParam("xmlid") final String xmlid )
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				final JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();
				jg.writeStringField("document_xmlid", xmlid);
				jg.writeFieldName( "metadata" );
				jg.writeStartObject();

				String query = "START document=node:Document('xmlid:"+xmlid+"') "
						+ "MATCH (document)-[r:HAS_METADATUM]->(m:Metadatum) "+
						"WITH r.value AS v, m.group AS g, m.key AS k "+
						"RETURN g AS group, k AS key, COLLECT(DISTINCT v) AS values "+
						"ORDER BY group, key;";
				
				String currentGroup = null;
				Map<String,Object> groupData = new HashMap<String,Object>();

				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					while ( result.hasNext() ) {
						Map<String,Object> row = result.next();
						String group = null;
						String key = null;
						Object values = null;
						
						for ( Entry<String, Object> column : row.entrySet() ) {
							String field = column.getKey();
							if (field.equals("values")){
								values = column.getValue();
							} else if (field.equals("group")) {
								group = (String) column.getValue();
							} else if (field.equals("key")) {
								key = (String) column.getValue();
							}
						}
						
						if (currentGroup == null || !group.equals(currentGroup)) {
							if (groupData.size() > 0) {
								writeGroup(currentGroup, groupData, jg);
								groupData = new HashMap<String,Object>();
							}
							currentGroup = group;
						}
						
						groupData.put(key, values);
					}
				}
				
				writeGroup(currentGroup, groupData, jg);
				jg.writeEndObject();
				jg.writeEndObject();
				jg.flush();
				jg.close();
			}

			private void writeGroup(String currentGroup, Map<String, Object> groupData, JsonGenerator jg) throws IOException {
				jg.writeFieldName(currentGroup);
				jg.writeStartObject();
				
				for (String k : groupData.keySet()) {
					jg.writeObjectField(k, groupData.get(k));
				}
				
				jg.writeEndObject();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	@GET
	@Path( "/{xmlid}/statistics" )
	public Response getDocumentStatistics( @PathParam("xmlid") final String xmlid )
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				final JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();
				jg.writeStringField("document_xmlid", xmlid);
				jg.writeFieldName( "statistics" );
				jg.writeStartObject();

				String query = "START document=node:Document('xmlid:"+xmlid+"') "
						+ "MATCH (document)-[:HAS_TOKEN]->(t:WordToken) "
						+ "MATCH (t)-[:HAS_TYPE]->(w:WordType) "
						+ "MATCH (t)-[:HAS_LEMMA]->(l:Lemma) "
						+ "WITH document.token_count AS token_count, COUNT(DISTINCT w) AS type_count, COUNT(DISTINCT l) AS lemma_count "
						+ "RETURN token_count, type_count, lemma_count, 1 / (token_count / type_count) AS type_token_ratio;";

				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					while ( result.hasNext() ) {
						Map<String,Object> row = result.next();
						Object token_count = null;
						Object type_count = null;
						Object lemma_count = null;
						
						for ( Entry<String, Object> column : row.entrySet() ) {
							String field = column.getKey();
							Object value = column.getValue();
							if (field.equals("token_count")){
								token_count = value;
							} else if (field.equals("type_count")) {
								type_count = value;
							} else if (field.equals("lemma_count")) {
								lemma_count = value;
							}
						}

						jg.writeObjectField("token_count", token_count);
						jg.writeObjectField("type_count", type_count);
						jg.writeObjectField("lemma_count", lemma_count);
					}
				}
				
				jg.writeEndObject();
				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

}
