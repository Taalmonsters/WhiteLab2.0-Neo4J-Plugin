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

@Path("/grouped_hits")
public class GroupedHitSearch extends DatabasePlugin {

	public GroupedHitSearch(@Context GraphDatabaseService database) {
		super(database);
	}
	
	@POST
	public Response doPost
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset,
			  @DefaultValue("hit_count") @QueryParam("sort") final String sort,
			  @DefaultValue("desc") @QueryParam("order") final String order, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("group") final String group )
	{
		System.out.println("GroupedHitSearch.doPost");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, null, sort, order, null, 8, number, offset, 0, false));
	}

	@GET
	public Response doGet
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("50") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @DefaultValue("hit_count") @QueryParam("sort") final String sort,
			  @DefaultValue("desc") @QueryParam("order") final String order, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("group") final String group )
	{
		System.out.println("GroupedHitSearch.doGet");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, null, sort, order, null, 8, number, offset, 0, false));
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
		System.out.println("GroupedHitSearch.doGetCount");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, null, null, null, null, 8, number, offset, 0, true));
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
		System.out.println("GroupedHitSearch.doPostCount");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, null, null, null, null, 8, number, offset, 0, true));
	}

	@GET
	@Path( "/hits" )
	public Response doGetGroupHits
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @QueryParam("query") final String jsonQueryString,
			  @DefaultValue("20") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("docpid") final String docPid,
			  @QueryParam("group") final String group,
			  @QueryParam("selected_group") final String selected_group )
	{
		System.out.println("GroupedHitSearch.doGetGroupHits");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, selected_group, null, null, null, 8, number, offset, 5, false));
	}

	@POST
	@Path( "/hits" )
	public Response doPostGroupHits
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @QueryParam("query") final String jsonQueryString,
			  @DefaultValue("20") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId,
			  @QueryParam("docpid") final String docPid,
			  @QueryParam("group") final String group,
			  @QueryParam("selected_group") final String selected_group )
	{
		System.out.println("GroupedHitSearch.doPostGroupHits");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, group, selected_group, null, null, null, 8, number, offset, 5, false));
	}

}
