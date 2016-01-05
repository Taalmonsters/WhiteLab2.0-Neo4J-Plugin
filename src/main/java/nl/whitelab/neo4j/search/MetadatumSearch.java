package nl.whitelab.neo4j.search;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import nl.whitelab.neo4j.searchers.MetadatumSearcher;

import org.neo4j.graphdb.GraphDatabaseService;

@Path("/metadata")
public class MetadatumSearch extends MetadatumSearcher {

	public MetadatumSearch(@Context GraphDatabaseService database) {
		super(database);
	}

	@GET
	public Response doGet( 
			@DefaultValue("10") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("group") @QueryParam("sort") final String sort, 
			@DefaultValue("asc") @QueryParam("order") final String order )
	{
		System.out.println("MetadatumSearch.doGet");
		return getMetadata(number, offset, sort, order);
	}

	@POST
	public Response doPost( 
			@DefaultValue("10") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("group") @QueryParam("sort") final String sort, 
			@DefaultValue("asc") @QueryParam("order") final String order )
	{
		System.out.println("MetadatumSearch.doPost");
		return getMetadata(number, offset, sort, order);
	}

	@POST
	@Path( "/{label}/update" )
	public Response doPostUpdate( 
			@PathParam("label") final String label,
			@QueryParam("property") final String property, 
			@QueryParam("value") final String value )
	{
		System.out.println("MetadatumSearch.doPostUpdate("+property+","+value+")");
		return updateMetadataLabel(label, property, value);
	}
	
	@GET
	@Path( "/{label}" )
	public Response doGetLabel(@PathParam("label") final String label) {
		System.out.println("MetadatumSearch.doGetLabel");
		return getMetadataLabel(label);
	}
	
	@POST
	@Path( "/{label}" )
	public Response doPostLabel(@PathParam("label") final String label) {
		System.out.println("MetadatumSearch.doPostLabel");
		return getMetadataLabel(label);
	}
	
	@GET
	@Path( "/{label}/values" )
	public Response doGetLabelValues(
			@DefaultValue("10") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("document_count") @QueryParam("sort") final String sort, 
			@DefaultValue("asc") @QueryParam("order") final String order, 
			@DefaultValue("true") @QueryParam("count") final Boolean addCounts, 
			@PathParam("label") final String label) {
		System.out.println("MetadatumSearch.doGetLabel");
		if (addCounts)
			return getMetadataLabelValuesWithCounts(label, number, offset, sort, order);
		else
			return getMetadataLabelValues(label, number, offset, sort, order);
	}
	
	@POST
	@Path( "/{label}/values" )
	public Response doPostLabelValues(
			@DefaultValue("10") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("document_count") @QueryParam("sort") final String sort, 
			@DefaultValue("asc") @QueryParam("order") final String order, 
			@DefaultValue("true") @QueryParam("count") final Boolean addCounts, 
			@PathParam("label") final String label) {
		System.out.println("MetadatumSearch.doPostLabel");
		if (addCounts)
			return getMetadataLabelValuesWithCounts(label, number, offset, sort, order);
		else
			return getMetadataLabelValues(label, number, offset, sort, order);
	}
	
	@GET
	@Path( "/{group}/{key}" )
	public Response doGetGroupKey(@PathParam("group") final String group, @PathParam("key") final String key) {
		System.out.println("MetadatumSearch.doGetGroupKey");
		return getMetadataGroupKey(group, key);
	}

	@POST
	@Path( "/{group}/{key}" )
	public Response doPostGroupKey(@PathParam("group") final String group, @PathParam("key") final String key) {
		System.out.println("MetadatumSearch.doPostGroupKey");
		return getMetadataGroupKey(group, key);
	}
	
	@GET
	@Path( "/{group}/{key}/values" )
	public Response doGetGroupKeyValues(
			@DefaultValue("10") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("value") @QueryParam("sort") final String sort, 
			@DefaultValue("asc") @QueryParam("order") final String order, 
			@DefaultValue("true") @QueryParam("count") final Boolean addCounts, 
			@PathParam("group") final String group, 
			@PathParam("key") final String key) {
		System.out.println("MetadatumSearch.doGetGroupKeyValues");
		if (addCounts)
			return getMetadataGroupKeyValuesWithCounts(group, key, number, offset, sort, order);
		else
			return getMetadataGroupKeyValues(group, key, number, offset, sort, order);
	}

	@POST
	@Path( "/{group}/{key}/values" )
	public Response doPostGroupKeyValues(
			@DefaultValue("10") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("value") @QueryParam("sort") final String sort, 
			@DefaultValue("asc") @QueryParam("order") final String order, 
			@DefaultValue("true") @QueryParam("count") final Boolean addCounts, 
			@PathParam("group") final String group, 
			@PathParam("key") final String key) {
		System.out.println("MetadatumSearch.doPostGroupKeyValues");
		if (addCounts)
			return getMetadataGroupKeyValuesWithCounts(group, key, number, offset, sort, order);
		else
			return getMetadataGroupKeyValues(group, key, number, offset, sort, order);
	}
	
	@GET
	@Path( "/{group}/{key}/value_documents" )
	public Response doGetGroupKeyValueDocuments(
			@QueryParam("value") final String value, 
			@PathParam("group") final String group, 
			@PathParam("key") final String key) {
		System.out.println("MetadatumSearch.doGetGroupKeyValueDocuments");
		return getMetadataGroupKeyValueDocumentsWithCounts(group, key, value);
	}
	
	@POST
	@Path( "/{group}/{key}/value_documents" )
	public Response doPostGroupKeyValueDocuments(
			@QueryParam("value") final String value, 
			@PathParam("group") final String group, 
			@PathParam("key") final String key) {
		System.out.println("MetadatumSearch.doPostGroupKeyValueDocuments");
		return getMetadataGroupKeyValueDocumentsWithCounts(group, key, value);
	}
	
	@GET
	@Path( "/content" )
	public Response doGetContent
			( @DefaultValue("document") @QueryParam("within") final String within, 
			  @DefaultValue("100000") @QueryParam("number") final Integer number, 
			  @DefaultValue("0") @QueryParam("offset") final Integer offset, 
			  @QueryParam("query") final String jsonQueryString,
			  @QueryParam("pattern") final String cqlPattern,
			  @QueryParam("filter") final String filter,
			  @QueryParam("query_id") final String queryId )
	{
		System.out.println("MetadatumSearch.doGetContent");
		return getQueryResult(constructor.parseQuery(queryId, within, jsonQueryString, cqlPattern, filter, null, null, null, null, null, 4, number, offset, 5, false));
	}
	
	@GET
	@Path( "/{group}/{key}/bubble" )
	public Response doGetAverageDocumentSizeVsTotalSize(
			@PathParam("group") final String group, 
			@PathParam("key") final String key,
			@QueryParam("filter") final String filter) {
		System.out.println("MetadatumSearch.doGetAverageDocumentSizeVsTotalSize");
		return getQueryResult(constructor.parseQuery(null, null, null, null, filter, group+"_"+key, null, null, null, null, 20, 0, 0, 5, false));
//		return getMetadataGroupKeyAverageDocumentSizeVsTotalSize(group, key, filter);
	}
	
	@POST
	@Path( "/{group}/{key}/bubble" )
	public Response doPostAverageDocumentSizeVsTotalSize(
			@PathParam("group") final String group, 
			@PathParam("key") final String key,
			@QueryParam("filter") final String filter) {
		System.out.println("MetadatumSearch.doPostAverageDocumentSizeVsTotalSize");
		return getQueryResult(constructor.parseQuery(null, null, null, null, filter, group+"_"+key, null, null, null, null, 20, 0, 0, 5, false));
//		return getMetadataGroupKeyAverageDocumentSizeVsTotalSize(group, key, filter);
	}

}
