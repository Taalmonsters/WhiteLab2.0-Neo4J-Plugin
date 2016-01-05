package nl.whitelab.neo4j.explore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@Path("/corpora")
public class CorpusExplore {
	private final GraphDatabaseService database;
	private final ObjectMapper objectMapper;

	public CorpusExplore(@Context GraphDatabaseService database) {
		this.database = database;
		this.objectMapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
				  							  .configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true)
				  							  .configure(SerializationFeature.INDENT_OUTPUT, true);
	}
	
	@GET
	public Response getAbsoluteTokenCounts( 
			@QueryParam("corpus") final String corpus,
			@QueryParam("collection") final String collection )
	{
		System.out.println("getAbsoluteTokenCounts");
		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();
				
				String query = "MATCH (corpus:Corpus) RETURN corpus.title AS name, corpus.token_count AS token_count;";
				if (corpus != null && corpus.length() > 0) {
					jg.writeStringField("corpus", corpus);
					if (collection != null && collection.length() > 0) {
						jg.writeStringField("collection", collection);
						query = "MATCH (corpus:Corpus{title:'"+corpus+"'}) MATCH (corpus)-[:HAS_COLLECTION]->(collection:Collection{title:'"+collection+"'}) MATCH (collection)-[:HAS_DOCUMENT]->(document:Document) RETURN document.xmlid AS name, document.token_count AS token_count;";
					} else {
						query = "MATCH (corpus:Corpus{title:'"+corpus+"'}) MATCH (corpus)-[:HAS_COLLECTION]->(collection:Collection) RETURN collection.title AS name, collection.token_count AS token_count;";
					}
				}

				System.out.println(query);
				jg.writeStringField("cypher", query);
				jg.writeFieldName( "result" );
				jg.writeStartObject();
				if (corpus != null && corpus.length() > 0) {
					if (collection != null && collection.length() > 0)
						jg.writeStringField("name", "Document_xmlid");
					else
						jg.writeStringField("name", "Collection_title");
				} else
					jg.writeStringField("name", "Corpus_title");
				jg.writeFieldName( "children" );
				jg.writeStartArray();
				jg.flush();
				
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					while ( result.hasNext() ) {
						jg.writeStartObject();
						Map<String,Object> row = result.next();
						for ( Entry<String, Object> column : row.entrySet() ) {
							String field = column.getKey();
							Object value = (Object) column.getValue();
							if (field.contains("_count") && value instanceof Integer)
								jg.writeNumberField("size", (Integer) value);
							else if (field.contains("_count") && value instanceof Long)
								jg.writeNumberField("size", (Long) value);
							else
								jg.writeStringField("name", (String) value);
						}
						jg.writeEndObject();
						jg.flush();
					}
				}

				jg.writeEndArray();
				jg.writeEndObject();
				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

}
