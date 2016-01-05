package nl.whitelab.neo4j.search;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import nl.whitelab.neo4j.DatabasePlugin;

import org.neo4j.graphdb.GraphDatabaseService;

@Path("/grouped_docs")
public class GroupedDocSearch extends DatabasePlugin {

	public GroupedDocSearch(@Context GraphDatabaseService database) {
		super(database);
	}
	
	@POST
	public Response doPost
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @DefaultValue("document_count") @QueryParam("sort") final String sort,
			  @DefaultValue("desc") @QueryParam("order") final String order, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("group") final String group )
	{
		System.out.println("GroupedDocSearcher.doPost");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, null, sort, order, null, 16, number, offset, 0, false));
	}

	@GET
	public Response doGet
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @DefaultValue("document_count") @QueryParam("sort") final String sort,
			  @DefaultValue("desc") @QueryParam("order") final String order, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("group") final String group )
	{
		System.out.println("GroupedDocSearcher.doGet");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, null, sort, order, null, 16, number, offset, 0, false));
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
			  @QueryParam("group") final String group )
	{
		System.out.println("GroupedDocSearcher.doGetCount");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, null, null, null, null, 16, number, offset, 0, true));
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
			  @QueryParam("group") final String group )
	{
		System.out.println("GroupedDocSearcher.doPostCount");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, null, null, null, null, 16, number, offset, 0, true));
	}
	
	@POST
	@Path( "/docs" )
	public Response doPostGroupDocs
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("group") final String group,
			  @QueryParam("selected_group") final String selected_group )
	{
		System.out.println("GroupedDocSearcher.doPost");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, selected_group, null, null, null, 16, number, offset, 5, false));
	}

	@GET
	@Path( "/docs" )
	public Response doGetGroupDocs
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("group") final String group,
			  @QueryParam("selected_group") final String selected_group )
	{
		System.out.println("GroupedDocSearcher.doGet");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, selected_group, null, null, null, 16, number, offset, 5, false));
	}

}
