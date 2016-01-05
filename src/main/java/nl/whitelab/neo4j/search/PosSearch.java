package nl.whitelab.neo4j.search;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import nl.whitelab.neo4j.database.LinkLabel;
import nl.whitelab.neo4j.database.NodeLabel;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@Path("/pos")
public class PosSearch {
	private final GraphDatabaseService database;
	private final ObjectMapper objectMapper;

	public PosSearch(@Context GraphDatabaseService database) {
		this.database = database;
		this.objectMapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
				  							  .configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true)
				  							  .configure(SerializationFeature.INDENT_OUTPUT, true);
	}
	
	@GET
	@Path("/tags")
	public Response getPosTags( 
			@DefaultValue("10") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("label") @QueryParam("sort") final String sort, 
			@DefaultValue("asc") @QueryParam("order") final String order )
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();
				
				String totalQuery = "MATCH (p:PosTag) RETURN COUNT(p)";
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(totalQuery) ) {
					Map<String,Object> row = result.next();
					Entry<String, Object> column = row.entrySet().iterator().next();
					jg.writeNumberField("total", (Long) column.getValue());
				}
				
				jg.writeNumberField("number", number);
				jg.writeNumberField("offset", offset);
				jg.writeStringField("sort", sort);
				jg.writeStringField("order", order);
				jg.writeFieldName( "pos_tags" );
				jg.writeStartArray();
				jg.flush();
				
				String sortString = "p."+sort;
				if (!sort.equals("label") && !sort.startsWith("token_count"))
					sortString = "p.label";
				
				String orderString = order;
				if (!order.toLowerCase().equals("asc") && !order.toLowerCase().equals("desc"))
					orderString = "asc";
				
				String query = "MATCH (p:PosTag) RETURN p "
						+ "ORDER BY "+sortString+" "+orderString+" "
						+ "SKIP "+String.valueOf(offset);
				
				if (number > 0)
					query = query + " LIMIT "+String.valueOf(number);
				
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					while ( result.hasNext() ) {
						jg.writeStartObject();
						Map<String,Object> row = result.next();
						for ( Entry<String, Object> column : row.entrySet() ) {
							Node posTag = (Node) column.getValue();
							for (String field : posTag.getPropertyKeys()) {
								Object value = posTag.getProperty(field);
								if (field.contains("_count") && value instanceof Integer)
									jg.writeNumberField(field, (Integer) value);
								else if (field.contains("_count") && value instanceof Long)
									jg.writeNumberField(field, (Long) value);
								else
									jg.writeStringField(field, (String) posTag.getProperty(field));
							}
						}
						jg.writeEndObject();
						jg.flush();
					}
				}

				jg.writeEndArray();
				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}
	
	@GET
	@Path("/heads")
	public Response getPosHeads( 
			@DefaultValue("12") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("label") @QueryParam("sort") final String sort, 
			@DefaultValue("asc") @QueryParam("order") final String order )
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();
				
				String totalQuery = "MATCH (p:PosHead) RETURN COUNT(p)";
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(totalQuery) ) {
					Map<String,Object> row = result.next();
					Entry<String, Object> column = row.entrySet().iterator().next();
					jg.writeNumberField("total", (Long) column.getValue());
				}
				
				jg.writeNumberField("number", number);
				jg.writeNumberField("offset", offset);
				jg.writeStringField("sort", sort);
				jg.writeStringField("order", order);
				jg.writeFieldName( "pos_heads" );
				jg.writeStartArray();
				jg.flush();
				
				String sortString = "p."+sort;
				if (!sort.equals("label") && !sort.startsWith("token_count"))
					sortString = "p.label";
				
				String orderString = order;
				if (!order.toLowerCase().equals("asc") && !order.toLowerCase().equals("desc"))
					orderString = "asc";
				
				String query = "MATCH (p:PosHead) RETURN p "
						+ "ORDER BY "+sortString+" "+orderString+" "
						+ "SKIP "+String.valueOf(offset);
				
				if (number > 0)
					query = query + " LIMIT "+String.valueOf(number);
				
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					while ( result.hasNext() ) {
						jg.writeStartObject();
						Map<String,Object> row = result.next();
						for ( Entry<String, Object> column : row.entrySet() ) {
							Node posTag = (Node) column.getValue();
							for (String field : posTag.getPropertyKeys()) {
								Object value = posTag.getProperty(field);
								if (field.contains("_count") && value instanceof Integer)
									jg.writeNumberField(field, (Integer) value);
								else if (field.contains("_count") && value instanceof Long)
									jg.writeNumberField(field, (Long) value);
								else
									jg.writeStringField(field, (String) posTag.getProperty(field));
							}
						}
						jg.writeEndObject();
						jg.flush();
					}
				}

				jg.writeEndArray();
				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	@GET
	@Path("/tags/{label}")
	public Response getPosTagByLabel(@PathParam("label") final String label)
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				try ( Transaction ignored = database.beginTx() ) {
					JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
					IndexManager index = database.index();
					Index<Node> postags = index.forNodes("PosTag");
					IndexHits<Node> hits = postags.get( "label", label );
					Node postag = hits.getSingle();
					jg.writeStartObject();
					for (String field : postag.getPropertyKeys()) {
						Object value = postag.getProperty(field);
						if (field.contains("_count") && value instanceof Integer)
							jg.writeNumberField(field, (Integer) value);
						else if (field.contains("_count") && value instanceof Long)
							jg.writeNumberField(field, (Long) value);
						else
							jg.writeStringField(field, (String) postag.getProperty(field));
					}
					jg.writeEndObject();
					jg.flush();
					jg.close();
				}
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	@GET
	@Path("/heads/{label}")
	public Response getPosHeadByLabel(@PathParam("label") final String label)
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				try ( Transaction ignored = database.beginTx() ) {
					JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
					IndexManager index = database.index();
					Index<Node> postags = index.forNodes("PosHead");
					IndexHits<Node> hits = postags.get( "label", label );
					Node poshead = hits.getSingle();
					jg.writeStartObject();
					for (String field : poshead.getPropertyKeys()) {
						Object value = poshead.getProperty(field);
						if (field.contains("_count") && value instanceof Integer)
							jg.writeNumberField(field, (Integer) value);
						else if (field.contains("_count") && value instanceof Long)
							jg.writeNumberField(field, (Long) value);
						else
							jg.writeStringField(field, (String) poshead.getProperty(field));
					}
					jg.writeEndObject();
					jg.flush();
					jg.close();
				}
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	@GET
	@Path("/tags/{label}/features")
	public Response getPosTagFeaturesByLabel(@PathParam("label") final String label)
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartArray();
				
				String query = "MATCH (p:PosTag{label:'"+label+"'}) "
						+ "WITH DISTINCT p "
						+ "OPTIONAL MATCH (p)-[:HAS_FEATURE]->(f:PosFeature) "
						+ "RETURN CASE WHEN COUNT(f) > 0 THEN COLLECT(DISTINCT {key: f.key, value: f.value}) ELSE [] END AS feats";
				
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					Map<String,Object> row = result.next();
					Entry<String, Object> column = row.entrySet().iterator().next();
					@SuppressWarnings("unchecked")
					Iterator<Map<String,String>> feats = ((Iterable<Map<String,String>>) column.getValue()).iterator();
					
					if (feats.hasNext()) {
						while (feats.hasNext()) {
							Map<String,String> feat = feats.next();
							jg.writeStartObject();
							jg.writeStringField("key", feat.get("key"));
							jg.writeStringField("value", feat.get("value"));
							jg.writeEndObject();
						}
					} else {
						Pattern pattern = Pattern.compile("^([A-Z]+)\\((.+)\\)");
						Matcher matcher = pattern.matcher(label);
						if (matcher.matches()) {
							String head = matcher.group(1);
							String inner = matcher.group(2);
							String[] parts = inner.split(",");
							IndexManager index = database.index();
							Index<Node> posfeats = index.forNodes("PosFeature");
							Index<Node> postags = index.forNodes("PosTag");
							Node mainPosTag = postags.get("label", label).next();
							for (int i = 0; i < parts.length; i++) {
								IndexHits<Node> hits = posfeats.get( "value", parts[i] );
								if (hits.hasNext()) {
									System.out.println("Found matching features in index ("+parts[i]+").");
									List<Node> noMatchingHead = new ArrayList<Node>();
									boolean processed = false;
									while (hits.hasNext()) {
										Node posFeature = hits.next();
										Node posTag = posFeature.getRelationships(LinkLabel.HAS_FEATURE, Direction.INCOMING).iterator().next().getOtherNode(posFeature);
										if (((String) posTag.getProperty("label")).startsWith(head+"(")) {
											mainPosTag.createRelationshipTo(posFeature, LinkLabel.HAS_FEATURE);
											posFeature.setProperty("pos_tag_count", (Long) posFeature.getProperty("pos_tag_count") + 1);
											posFeature.setProperty("token_count", (int) mainPosTag.getProperty("token_count") + 1);
											jg.writeStartObject();
											jg.writeStringField("key", (String) posFeature.getProperty("key"));
											jg.writeStringField("value", (String) posFeature.getProperty("value"));
											jg.writeEndObject();
											processed = true;
											break;
										} else
											noMatchingHead.add(posFeature);
									}
									if (!processed && noMatchingHead.size() > 0) {
										Node posFeature = noMatchingHead.get(0);
										mainPosTag.createRelationshipTo(posFeature, LinkLabel.HAS_FEATURE);
										posFeature.setProperty("pos_tag_count", (Long) posFeature.getProperty("pos_tag_count") + 1);
										posFeature.setProperty("token_count", (int) mainPosTag.getProperty("token_count") + 1);
										jg.writeStartObject();
										jg.writeStringField("key", (String) posFeature.getProperty("key"));
										jg.writeStringField("value", (String) posFeature.getProperty("value"));
										jg.writeEndObject();
									}
								} else {
									System.out.println("No matching features in index ("+parts[i]+"). Creating new feature.");
									Node posFeature = database.createNode(NodeLabel.PosFeature);
									posFeature.setProperty("label", parts[i]+"="+parts[i]);
									posFeature.setProperty("key", parts[i]);
									posFeature.setProperty("value", parts[i]);
									posFeature.setProperty("pos_tag_count", 1);
									posFeature.setProperty("token_count", (int) mainPosTag.getProperty("token_count"));
									posfeats.add(posFeature, "label", parts[i]+"="+parts[i]);
									posfeats.add(posFeature, "key", parts[i]);
									posfeats.add(posFeature, "value", parts[i]);
									mainPosTag.createRelationshipTo(posFeature, LinkLabel.HAS_FEATURE);
									jg.writeStartObject();
									jg.writeStringField("key", (String) posFeature.getProperty("key"));
									jg.writeStringField("value", (String) posFeature.getProperty("value"));
									jg.writeEndObject();
								}
							}
						}
					}
				}
				
				jg.writeEndArray();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	@GET
	@Path("/tags/{label}/word_types")
	public Response getPosTagWordTypesByLabel(
			@PathParam("label") final String label,
			@DefaultValue("10") @QueryParam("number") final Integer number, 
			@DefaultValue("0") @QueryParam("offset") final Integer offset, 
			@DefaultValue("label") @QueryParam("sort") final String sort, 
			@DefaultValue("asc") @QueryParam("order") final String order
		)
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartObject();
				jg.writeNumberField("number", number);
				jg.writeNumberField("offset", offset);
				jg.writeStringField("sort", sort);
				jg.writeStringField("order", order);
				jg.writeFieldName( "word_types" );
				jg.writeStartArray();
				jg.flush();
				
				String sortString = sort;
				if (!sort.equals("word_type") && !sort.equals("token_count"))
					sortString = "word_type";
				
				String orderString = order;
				if (!order.toLowerCase().equals("asc") && !order.toLowerCase().equals("desc"))
					orderString = "asc";
				
				String query = "MATCH (p:PosTag{label:'"+label+"'}) "
						+ "WITH DISTINCT p "
						+ "MATCH (p)<-[:HAS_POS_TAG]-(t:WordToken)-[:HAS_TYPE]->(w:WordType) "
						+ "RETURN DISTINCT w.label AS word_type, COUNT(DISTINCT t) AS token_count "
						+ "ORDER BY "+sortString+" "+orderString+" "
						+ "SKIP "+String.valueOf(offset);
				
				if (number > 0)
					query = query + " LIMIT "+String.valueOf(number);
				
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					while ( result.hasNext() ) {
						jg.writeStartObject();
						Map<String,Object> row = result.next();
						for ( Entry<String, Object> column : row.entrySet() ) {
							String field = column.getKey();
							if (field.equals("word_type"))
								jg.writeStringField("word_type", (String) column.getValue());
							else
								jg.writeNumberField("token_count", (Long) column.getValue());
						}
						jg.writeEndObject();
						jg.flush();
					}
				}

				jg.writeEndArray();
				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	@GET
	@Path("/heads/{label}/features")
	public Response getPosHeadFeaturesByLabel(@PathParam("label") final String label)
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@SuppressWarnings("unchecked")
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartArray();
				
				String query = "MATCH (p:PosTag) WHERE p.label =~ '"+label+"(.*)'	"
						+ "WITH DISTINCT p "
						+ "MATCH (p)-[:HAS_FEATURE]->(f:PosFeature) "
						+ "RETURN DISTINCT f.key AS feature_key, COLLECT(DISTINCT f.value) AS feature_values "
						+ "ORDER BY feature_key";
				
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					while ( result.hasNext() ) {
						Map<String,Object> row = result.next();
						String key = null;
						Iterable<String> values = null;
						for ( Entry<String, Object> column : row.entrySet() ) {
							String field = column.getKey();
							if (field.equals("feature_key"))
								key = (String) column.getValue();
							else {
								values = (Iterable<String>) column.getValue();
							}
						}
						
						if (key != null && values != null) {
							jg.writeStartObject();
							jg.writeFieldName(key);
							jg.writeStartArray();
							Iterator<String> it = values.iterator();
							while (it.hasNext()) {
								String value = it.next();
								jg.writeString(value);
							}
							jg.writeEndArray();
							jg.writeEndObject();
							jg.flush();
						}
					}
				}
				
				jg.writeEndArray();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}

	@GET
	@Path("/heads/{label}/tags")
	public Response getPosHeadTagsByLabel(@PathParam("label") final String label)
	{
		StreamingOutput stream = new StreamingOutput()
		{
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException
			{
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.writeStartArray();
				
				String query = "MATCH (p:PosTag) WHERE p.label =~ '"+label+"(.*)' "
						+ "WITH DISTINCT p ORDER BY p.label "
						+ "RETURN p AS pos_tag";
				
				try ( Transaction ignored = database.beginTx(); Result result = database.execute(query) ) {
					while ( result.hasNext() ) {
						jg.writeStartObject();
						Map<String,Object> row = result.next();
						for ( Entry<String, Object> column : row.entrySet() ) {
							Node posTag = (Node) column.getValue();
							for (String field : posTag.getPropertyKeys()) {
								Object value = posTag.getProperty(field);
								if (field.contains("_count") && value instanceof Integer)
									jg.writeNumberField(field, (Integer) value);
								else if (field.contains("_count") && value instanceof Long)
									jg.writeNumberField(field, (Long) value);
								else
									jg.writeStringField(field, (String) posTag.getProperty(field));
							}
						}
						jg.writeEndObject();
						jg.flush();
					}
				}
				
				jg.writeEndArray();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}
}
