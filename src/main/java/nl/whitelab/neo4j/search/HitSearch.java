package nl.whitelab.neo4j.search;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
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
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@Path("/hits")
public class HitSearch extends DatabasePlugin {
	protected final ObjectMapper objectMapper;

	public HitSearch(@Context GraphDatabaseService database) {
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
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("docpid") final String docPid )
	{
		System.out.println("HitSearch.doPost");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, null, null, null, null, docPid, 1, number, offset, 5, false));
	}

	@GET
	public Response doGet
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("docpid") final String docPid )
	{
		System.out.println("HitSearch.doGet");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, null, null, null, null, docPid, 1, number, offset, 5, false));
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
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("docpid") final String docPid )
	{
		System.out.println("HitSearch.doGetCount");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, null, null, null, null, docPid, 1, number, offset, 0, true));
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
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("docpid") final String docPid )
	{
		System.out.println("HitSearch.doPostCount");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, null, null, null, null, docPid, 1, number, offset, 0, true));
	}

	@GET
	@Path( "/growth" )
	public Response doGetGrowth
			( @DefaultValue("50000") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("docpid") final String docPid )
	{
		System.out.println("HitSearch.doGetGrowth");
		return getQueryResult(constructor.parseQuery(queryId, "document", null, "[word=\".*\"]", filter, null, null, null, null, docPid, 20, number, offset, 5, false));
	}

	@POST
	@Path( "/growth" )
	public Response doPostGrowth
			( @DefaultValue("50000") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("docpid") final String docPid )
	{
		System.out.println("HitSearch.doPostGrowth");
		return getQueryResult(constructor.parseQuery(queryId, "document", null, "[word=\".*\"]", filter, null, null, null, null, docPid, 20, number, offset, 5, false));
	}

	@GET
	@Path( "/kwic" )
	public Response getKwic
			( @QueryParam("docpid") final String docpid, 
			  @DefaultValue("50") @QueryParam("size") final Integer size, 
			  @QueryParam("first_index") final Integer firstIndex, 
			  @QueryParam("last_index") final Integer lastIndex )
	{
		System.out.println("HitSearch.getKwic");
		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException {
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.setPrettyPrinter(new DefaultPrettyPrinter());
				jg.writeStartObject();

				Integer firstFirstIndex = java.lang.Math.max(1,firstIndex-size);
				Integer lastLastIndex = lastIndex+size;
				
				String query = "MATCH (d:Document{xmlid:'"+docpid+"'})-[:HAS_TOKEN]->(t:WordToken) "
						+ "WHERE t.token_index >= "+String.valueOf(firstFirstIndex)+" AND t.token_index < "+String.valueOf(firstIndex)+" "
						+ "WITH t ORDER BY t.token_index "
						+ "MATCH (t)-[:HAS_TYPE]->(w:WordType) "
						+ "WITH COLLECT(w.label) AS left_context "
						+ "MATCH (d:Document{xmlid:'"+docpid+"'})-[:HAS_TOKEN]->(t:WordToken) "
						+ "WHERE t.token_index >= "+String.valueOf(firstIndex)+" AND t.token_index <= "+String.valueOf(lastIndex)+" "
						+ "WITH left_context, t ORDER BY t.token_index "
						+ "WITH left_context, COLLECT(t) AS tt "
						+ "UNWIND tt AS t "
						+ "MATCH (t)-[:HAS_TYPE]->(w:WordType) "
						+ "WITH left_context, COLLECT(w.label) AS hit_text "
						+ "MATCH (d:Document{xmlid:'"+docpid+"'})-[:HAS_TOKEN]->(t:WordToken) "
						+ "WHERE t.token_index > "+String.valueOf(lastIndex)+" AND t.token_index <= "+String.valueOf(lastLastIndex)+" "
						+ "WITH left_context, hit_text, t ORDER BY t.token_index "
						+ "WITH left_context, hit_text, COLLECT(t) AS tt "
						+ "UNWIND tt AS t "
						+ "MATCH (t)-[:HAS_TYPE]->(w:WordType) "
						+ "RETURN left_context, hit_text, COLLECT(w.label) AS right_context";

				System.out.println(query);
				
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					if (result.hasNext()) {
						Map<String,Object> row = result.next();
						for ( Entry<String, Object> column : row.entrySet() ) {
							executor.writeField(column, jg);
						}
					}
				}
				jg.writeEndObject();
				
				jg.flush();
				jg.close();
			}
		};
		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

}
