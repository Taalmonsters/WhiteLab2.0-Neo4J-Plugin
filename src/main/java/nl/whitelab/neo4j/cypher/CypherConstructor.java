package nl.whitelab.neo4j.cypher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONObject;

import nl.whitelab.neo4j.DatabasePlugin;
import nl.whitelab.neo4j.util.CQLParser;
import nl.whitelab.neo4j.util.Query;

public class CypherConstructor {
	protected final CQLParser parser;
	private final DatabasePlugin plugin;
	
	public CypherConstructor(DatabasePlugin plugin) {
		this.parser = new CQLParser();
		this.plugin = plugin;
	}
	
	public String getCypher(Query query) {
		try {
			if (query.getView() < 20) {
				query = addPatternFrequencies(query);
				query = reorderQueryClauses(query);
			}
			CypherQuery cypherQuery = getQueryTemplate(query);
			cypherQuery.applyTemplate(query);
			String cypher = cypherQuery.toQueryString();
			System.out.println("*** INFO: Cypher: "+cypher);
			return cypher;
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			System.out.println("*** ERROR: Unable to retrieve CypherQuery template:");
			e.printStackTrace();
		}
		return null;
	}
	
	private Query addPatternFrequencies(Query query) {
		JSONObject columns = query.toJSON();
		Iterator<String> columnIt = columns.keys();
		while (columnIt.hasNext()) {
			String columnKey = columnIt.next();
			JSONObject column = columns.getJSONObject(columnKey);
			column = addPatternFrequenciesToColumn(column);
			columns.put(columnKey, column);
		}
		query.setJSON(columns);
		return query;
	}
	
	private JSONObject addPatternFrequenciesToColumn(JSONObject column) {
		if (column.has("fields")) {
			JSONArray fields = column.getJSONArray("fields");
			Long highestFrequency = (long) 0;
			for (int f = 0; f < fields.length(); f++) {
				JSONObject field = fields.getJSONObject(f);
				if (field.has("pattern")) {
					Long frequency = getFieldFrequency(field);
					field.put("frequency", frequency);
					if (frequency > highestFrequency)
						highestFrequency = frequency;
				} else if (field.has("fields")) {
					field = addPatternFrequenciesToColumn(field);
					Long frequency = field.getLong("highest_frequency");
					if (frequency > highestFrequency)
						highestFrequency = frequency;
				}
				fields.put(f, field);
			}
			column.put("fields", fields);
			column.put("highest_frequency", highestFrequency);
		}
		return column;
	}
	
	private Long getFieldFrequency(JSONObject field) {
		Long frequency = (long) 0;
		String pattern = field.getString("pattern");
		if (pattern.equals(".*") || pattern.equals("*")) { // Get total token count
			frequency = this.plugin.getExecutor().getTotalTokenCount();
		} else { // Get summed frequency from matching typed nodes
			String type = field.getString("field");
			Boolean sensitive = false;
			if (field.has("case_sensitive"))
				sensitive = field.getBoolean("case_sensitive");
			frequency = this.plugin.getExecutor().getTypedNodeTokenCount(type, pattern, sensitive);
		}
		return frequency;
	}
	
	private Query reorderQueryClauses(Query query) {
		JSONObject columns = query.toJSON();
		Iterator<String> columnIt = columns.keys();
		while (columnIt.hasNext()) {
			String columnKey = columnIt.next();
			JSONObject column = columns.getJSONObject(columnKey);
			column = reorderColumnClauses(column);
			columns.put(columnKey, column);
		}
		query.setJSON(columns);
		return query;
	}
	
	private JSONObject reorderColumnClauses(JSONObject column) {
		if (column.has("fields")) {
			JSONArray fields = column.getJSONArray("fields");
			Map<Long,List<JSONObject>> fieldsByFrequency = new HashMap<Long,List<JSONObject>>();
			List<JSONObject> fieldsWithoutFrequency = new ArrayList<JSONObject>();
			for (int f = 0; f < fields.length(); f++) {
				JSONObject field = fields.getJSONObject(f);
				Long frequency = (long) 0;
				if (field.has("pattern"))
					frequency = field.getLong("frequency");
				else if (field.has("fields")) {
					field = reorderColumnClauses(field);
					fields.put(f, field);
					frequency = field.getLong("highest_frequency");
				} else {
					fieldsWithoutFrequency.add(field);
				}
				if (!fieldsByFrequency.containsKey(frequency))
					fieldsByFrequency.put(frequency, new ArrayList<JSONObject>());
				fieldsByFrequency.get(frequency).add(field);
			}
			JSONArray reorderedFields = new JSONArray();
			SortedSet<Long> keys = new TreeSet<Long>(fieldsByFrequency.keySet());
			for (Long frequency : keys) {
				for (JSONObject field : fieldsByFrequency.get(frequency)) {
					reorderedFields.put(field);
				}
			}
			for (JSONObject field : fieldsWithoutFrequency)
				reorderedFields.put(field);
			
			column.put("fields", reorderedFields);
		}
		return column;
	}
	
	private CypherQuery getQueryTemplate(Query query) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Boolean emptyPattern = query.hasEmptyPattern();
		int view = query.getView();
		Class<?> templateClass = null;
		if (view == 1)
			if (emptyPattern && query.isCountQuery()) {
				query.setGroup("Corpus_title");
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.TokenCountFromNodesQueryTemplate");
			} else
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.HitsQueryTemplate");
		else if (view == 2)
			if (emptyPattern && query.isCountQuery()) {
				query.setGroup("Corpus_title");
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.DocumentCountFromNodesQueryTemplate");
			} else
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.DocsQueryTemplate");
		else if (view == 8) {
			System.out.println("CypherConstructor.emptyPattern: "+String.valueOf(emptyPattern));
			System.out.println("CypherConstructor.group: "+String.valueOf(query.getGroup()));
			if (emptyPattern && query.getGroup().startsWith("hit_"))
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.FrequencyListQueryTemplate");
			else if (emptyPattern && (query.getGroup().startsWith("Corpus") || query.getGroup().startsWith("Collection")))
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.TokenCountFromNodesQueryTemplate");
			else if (emptyPattern && !query.getGroup().startsWith("hit_") && !query.getGroup().endsWith("_left") && !query.getGroup().endsWith("_right") && !query.getGroup().startsWith("Corpus") && !query.getGroup().startsWith("Collection"))
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.TokenCountFromDocumentsQueryTemplate");
			else
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.GroupedHitsQueryTemplate");
		} else if (view == 16)
			if (emptyPattern && (query.getGroup().startsWith("Corpus") || query.getGroup().startsWith("Collection")))
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.DocumentCountFromNodesQueryTemplate");
			else
				templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.GroupedDocsQueryTemplate");
		else if (view == 24)
			templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.DocumentContentQueryTemplate");
		else if (view == 4)
			templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.VocabularyGrowthQueryTemplate");
		else if (view == 20)
			templateClass = Class.forName("nl.whitelab.neo4j.cypher.templates.TotalSizeVsDocumentSizeQueryTemplate");
		return (CypherQuery) templateClass.newInstance();
	}
	
	public Query parseQuery(String queryId, String within, String jsonQueryString, String cqlPattern, String filter, 
			String group, String selectedGroup, String sort, String order, String docPid, Integer view, Integer number, Integer offset, Integer contextSize, Boolean countQuery) {
		Query query = null;
		if ((cqlPattern != null && cqlPattern.length() > 0) || (jsonQueryString != null && jsonQueryString.length() > 0)) {
			if (jsonQueryString != null && jsonQueryString.length() > 0) {
				query = new Query(queryId, new JSONObject(jsonQueryString), within, filter, view, offset, number, contextSize, countQuery);
			} else if (cqlPattern != null && cqlPattern.length() > 0) {
				query = new Query(queryId, cqlPattern, within, filter, view, offset, number, contextSize, countQuery);
				query.setJSON(parser.CQLtoJSON(cqlPattern));
			}
			
			if (docPid != null && docPid.length() > 0)
				query.setDocpid(docPid);
			
			if (group != null && group.length() > 0)
				query.setGroup(group);
			
			if (selectedGroup != null && selectedGroup.length() > 0)
				query.setSelectedGroup(selectedGroup);
			
			if (sort != null && sort.length() > 0)
				query.setSort(sort);
			
			if (order != null && order.length() > 0)
				query.setOrder(order);

			query.setCypher(getCypher(query));
		} else if (view == 20) {
			query = new Query(queryId, cqlPattern, within, filter, view, offset, number, contextSize, countQuery);
			
			if (group != null && group.length() > 0)
				query.setGroup(group);

			query.setCypher(getCypher(query));
		} else if (view == 24) {
			query = new Query(queryId, cqlPattern, within, filter, view, offset, number, contextSize, countQuery);
			
			if (docPid != null && docPid.length() > 0)
				query.setDocpid(docPid);

			query.setCypher(getCypher(query));
		}
		return query;
	}

}
