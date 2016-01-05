package nl.whitelab.neo4j.cypher;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;

import scala.collection.convert.Wrappers.MapWrapper;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import nl.whitelab.neo4j.database.NodeLabel;
import nl.whitelab.neo4j.util.Query;

public class CypherExecutor {
	protected final GraphDatabaseService database;
	protected final ObjectMapper objectMapper;
	
	public CypherExecutor(GraphDatabaseService database) {
		this.database = database;
		this.objectMapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
				  .configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true)
				  .configure(SerializationFeature.INDENT_OUTPUT, true);
	}
	
	public Long getTotalTokenCount() {
		Long frequency = (long) 0;

		try ( Transaction ignored = database.beginTx(); ) {
			ResourceIterator<Node> it = database.findNodes(NodeLabel.NodeCounter);
			Node nodeCounter = it.next();
			Object count = nodeCounter.getProperty("word_token_count");
			if (count instanceof Integer)
				frequency = frequency + new Long((Integer) count);
			else if (count instanceof Long)
				frequency = frequency + (Long) count;
		}
		
		return frequency;
	}
	
	public Long getTypedNodeTokenCount(String type, String pattern, Boolean sensitive) {
		Long frequency = (long) 0;

		try ( Transaction ignored = database.beginTx(); ) {
			IndexManager index = database.index();
			Index<Node> nodeIndex = index.forNodes(type);
			for (Node match : nodeIndex.get("label", pattern)) {
				if (!sensitive || ((String) match.getProperty("label")).equals(pattern)) {
					Object count = match.getProperty("token_count");
					if (count instanceof Integer)
						frequency = frequency + new Long((Integer) count);
					else if (count instanceof Long)
						frequency = frequency + (Long) count;
				}
			}
		}
		
		return frequency;
	}
	
	public Response getQueryResult(final Query query) {
		StreamingOutput stream = new StreamingOutput() {
			@Override
			public void write( OutputStream os ) throws IOException, WebApplicationException {
				JsonGenerator jg = objectMapper.getFactory().createGenerator(os, JsonEncoding.UTF8);
				jg.setPrettyPrinter(new DefaultPrettyPrinter());
				jg.writeStartObject();
				if (query != null && query.toCypher().length() > 0) {
					writeQueryDetails(jg, query);
					System.out.println(query.toCypher());
					executeQuery(jg, query);
				} else {
					jg.writeStringField("error", "No query supplied.");
				}
				jg.writeEndObject();
				jg.flush();
				jg.close();
			}
		};

		return Response.ok().entity( stream ).type( MediaType.APPLICATION_JSON ).build();
	}
	
	private void writeQueryDetails(JsonGenerator jg, Query query) throws IOException {
		if (query.getView() != 20) {
			jg.writeStringField("id", query.getId());
			System.out.print("{ id: "+query.getId());
			jg.writeStringField("cql", query.toCQL());
			System.out.print(", cql: "+query.toCQL());
			if (query.toJSON() != null)
				jg.writeStringField("json", query.toJSON().toString());
			jg.writeStringField("within", query.getWithin());
			System.out.print(", within: "+query.getWithin());
			jg.writeNumberField("number", query.getLimit());
			System.out.print(", number: "+String.valueOf(query.getLimit()));
			jg.writeNumberField("offset", query.getSkip());
			System.out.print(", offset: "+String.valueOf(query.getSkip()));
		}
		jg.writeStringField("filter", query.getFilter());
		System.out.print(", filter: "+query.getFilter());
		jg.writeNumberField("view", query.getView());
		System.out.print(", view: "+String.valueOf(query.getView()));
		
		String group = query.getGroup();
		if (group != null && group.length() > 0) {
			jg.writeStringField("group", group);
			System.out.print(", group: "+group);
		}
		
		if (query.getView() != 20) {
			String sort = query.getSort();
			if (sort != null && sort.length() > 0) {
				jg.writeStringField("sort", sort);
				System.out.print(", sort: "+sort);
			}
			
			String order = query.getOrder();
			if (order != null && order.length() > 0) {
				jg.writeStringField("order", order);
				System.out.print(", order: "+order);
			}
			
			String docPid = query.getDocpid();
			if (docPid != null && docPid.length() > 0) {
				jg.writeStringField("docpid", docPid);
				System.out.print(", docpid: "+docPid);
			}
			System.out.print(", count: "+String.valueOf(query.isCountQuery())+" }");
			System.out.println("");
		}
		
		
		jg.writeStringField("cypher", query.toCypher());
		jg.flush();
	}
	
	private void executeQuery(JsonGenerator jg, Query query) throws IOException {
		query.setStart(System.currentTimeMillis());
		try ( Transaction ignored = database.beginTx(); Result result = database.execute(query.toCypher()) ) {
			query.setEnd(System.currentTimeMillis());
			jg.writeNumberField("duration", query.getDuration());
			if (!query.isCountQuery())
				jg.writeArrayFieldStart(query.getResultHeader());
			while ( result.hasNext() ) {
				Map<String, Object> row = result.next();
				if (!query.isCountQuery())
					jg.writeStartObject();
				for ( Entry<String, Object> column : row.entrySet() ) {
					writeField(column, jg);
				}
				if (!query.isCountQuery())
					jg.writeEndObject();
			}
			if (!query.isCountQuery())
				jg.writeEndArray();
		}
	}
	
	public List<Map<String, Object>> executeQuery(Query query) {
		List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
		try ( Transaction ignored = database.beginTx(); Result result = database.execute(query.toCypher()) ) {
			while ( result.hasNext() ) {
				Map<String, Object> row = result.next();
				Map<String, Object> resultRow = new HashMap<String, Object>();
				for (Entry<String, Object> column : row.entrySet() ) {
					resultRow.put(column.getKey(), column.getValue());
				}
				results.add(resultRow);
			}
		}
		return results;
	}
	
	public void writeField(Entry<String, Object> column, JsonGenerator jg) throws IOException {
		String field = column.getKey();
		Object value = column.getValue();
		writeValue(field, value, jg);
	}
	
	public void writeField(Node entity, JsonGenerator jg) throws IOException {
		jg.writeStartObject();
		for (String field : entity.getPropertyKeys()) {
			Object value = entity.getProperty(field);
			writeValue(field, value, jg);
		}
		jg.writeEndObject();
		jg.flush();
	}
	
	private void writeValue(String field, Object value, JsonGenerator jg) throws IOException {
		if (value instanceof String)
			jg.writeStringField(field, (String) value);
		else if (value instanceof Long)
			jg.writeNumberField(field, (Long) value);
		else if (value instanceof Integer)
			jg.writeNumberField(field, (Integer) value);
		else if (value instanceof Float)
			jg.writeNumberField(field, (Float) value);
		else if (value instanceof Double)
			jg.writeNumberField(field, (Double) value);
		else if (value instanceof Boolean)
			jg.writeBooleanField(field, (Boolean) value);
		else if (value instanceof Iterable<?>)
			writeIterableToField(field, (Iterable<?>) value, jg);
		else if (value instanceof MapWrapper<?,?>)
			writeMapWrapperToField(field, (MapWrapper<?,?>) value, jg);
		else
			jg.writeStringField(field, "Unknown");
		jg.flush();
	}
	
	private void writeMapWrapperToField(String field, MapWrapper<?,?> wrapper, JsonGenerator jg) throws IOException {
		if (field != null)
			jg.writeFieldName(field);
		jg.writeStartObject();
		for (Object key : wrapper.keySet()) {
			String subField = String.valueOf(key);
			Object wvalue = wrapper.get(key);
			writeValue(subField, wvalue, jg);
		}
		jg.writeEndObject();
	}
	
	private void writeIterableToField(String field, Iterable<?> elements, JsonGenerator jg) throws IOException {
		Iterator<?> it = elements.iterator();
		Object first = it.next();
		if (first instanceof String && field != null && field.contains("text"))
			writeIterableToStringField(field, elements, jg);
		else {
			if (field != null)
				jg.writeFieldName(field);
			jg.writeStartArray();
			
			if (first instanceof String)
				writeIterableToStringArray(elements, jg);
			else if (first instanceof Integer)
				writeIterableToIntegerArray(elements, jg);
			else if (first instanceof Long)
				writeIterableToLongArray(elements, jg);
			else if (first instanceof Float)
				writeIterableToFloatArray(elements, jg);
			else if (first instanceof Double)
				writeIterableToDoubleArray(elements, jg);
			else if (first instanceof Boolean)
				writeIterableToBooleanArray(elements, jg);
			else if (first instanceof Iterable<?>)
				writeIterableToField(null, (Iterable<?>) first, jg);
			else if (first instanceof MapWrapper<?,?>)
				writeIterableToMapWrapperArray(elements, jg);
			else
				System.out.println(String.valueOf(first.getClass()));
			
			jg.writeEndArray();
		}
	}
	
	private void writeIterableToStringField(String field, Iterable<?> elements, JsonGenerator jg) throws IOException {
		List<String> list = new ArrayList<String>();
		@SuppressWarnings("unchecked")
		Iterator<String> it = (Iterator<String>) elements.iterator();
		while (it.hasNext())
			list.add(it.next());
		jg.writeStringField(field, StringUtils.join(list.toArray(), " "));
	}
	
	private void writeIterableToStringArray(Iterable<?> elements, JsonGenerator jg) throws IOException {
		@SuppressWarnings("unchecked")
		Iterator<String> it = (Iterator<String>) elements.iterator();
		while (it.hasNext()) {
			jg.writeString(it.next());
		}
	}
	
	private void writeIterableToIntegerArray(Iterable<?> elements, JsonGenerator jg) throws IOException {
		@SuppressWarnings("unchecked")
		Iterator<Integer> it = (Iterator<Integer>) elements.iterator();
		while (it.hasNext()) {
			jg.writeNumber(it.next());
		}
	}
	
	private void writeIterableToLongArray(Iterable<?> elements, JsonGenerator jg) throws IOException {
		@SuppressWarnings("unchecked")
		Iterator<Long> it = (Iterator<Long>) elements.iterator();
		while (it.hasNext()) {
			jg.writeNumber(it.next());
		}
	}
	
	private void writeIterableToFloatArray(Iterable<?> elements, JsonGenerator jg) throws IOException {
		@SuppressWarnings("unchecked")
		Iterator<Float> it = (Iterator<Float>) elements.iterator();
		while (it.hasNext()) {
			jg.writeNumber(it.next());
		}
	}
	
	private void writeIterableToDoubleArray(Iterable<?> elements, JsonGenerator jg) throws IOException {
		@SuppressWarnings("unchecked")
		Iterator<Double> it = (Iterator<Double>) elements.iterator();
		while (it.hasNext()) {
			jg.writeNumber(it.next());
		}
	}
	
	private void writeIterableToBooleanArray(Iterable<?> elements, JsonGenerator jg) throws IOException {
		@SuppressWarnings("unchecked")
		Iterator<Boolean> it = (Iterator<Boolean>) elements.iterator();
		while (it.hasNext()) {
			jg.writeBoolean(it.next());
		}
	}
	
	private void writeIterableToMapWrapperArray(Iterable<?> elements, JsonGenerator jg) throws IOException {
		@SuppressWarnings("unchecked")
		Iterator<MapWrapper<?,?>> it = (Iterator<MapWrapper<?,?>>) elements.iterator();
		while (it.hasNext()) {
			writeMapWrapperToField(null, (MapWrapper<?,?>) it.next(), jg);
		}
	}

}
