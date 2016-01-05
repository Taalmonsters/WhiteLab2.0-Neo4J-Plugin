package nl.whitelab.neo4j.cypher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import nl.whitelab.neo4j.util.Query;

public abstract class CypherQuery {
	protected Query query = null;
	protected Boolean hasEmptyPattern = false;
	protected Boolean hasEmptyFilter = false;
	protected Boolean isCountQuery = false;
	protected Boolean documentNodeAdded = false;
//	protected List<Map<String, String>> startNodes = new ArrayList<Map<String, String>>();
	protected Map<String, String> startNodes = new HashMap<String, String>();
	protected Map<String, String> tokenPatterns = new HashMap<String, String>();
	private Map<String, List<String>> fieldKeysPerColumn = new HashMap<String, List<String>>();
	private List<String> processedKeys = new ArrayList<String>();
	protected List<String> tokenIds = new ArrayList<String>();
	protected List<String> connectedTokenIds = new ArrayList<String>();
	private List<String> columnTokenIds = new ArrayList<String>();
	private char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
	private int tokenCount = 0;
	protected String tokenPattern = "";
	protected String metadataFilter = "";
	protected String hitContent = "";
	protected String hitContext = "";
	protected String docHitsPattern = "";
	protected String filterGrouping = "";

	abstract public String toQueryString();

	abstract public void applyTemplate(Query query);
	
	protected void addStartNode(String type, String columnKey, Integer fieldCount, String valueFilter) {
		String nodeId = generateFieldKey(type, columnKey, fieldCount);
		String nodeDescription = "node:"+type+"('"+valueFilter+"')";
//		Map<String, String> start = new HashMap<String, String>();
//		start.put(nodeId, nodeDescription);
//		startNodes.add(start);
		startNodes.put(nodeId, nodeDescription);
	}
	
	protected String getStartNode(String type, String columnKey, Integer fieldCount) {
		return generateFieldKey(type, columnKey, fieldCount);
	}
	
	protected List<String> getStartNodesForType(String type) {
		List<String> matches = new ArrayList<String>();
		for (String nodeId : startNodes.keySet()) {
			if (nodeId.startsWith(typeToId(type)) && !matches.contains(nodeId))
				matches.add(nodeId);
		}
		return matches;
	}
	
	protected List<String> getStartNodesForTypes(String[] types) {
		List<String> matches = new ArrayList<String>();
		for (String type : types) {
			List<String> typeMatches = getStartNodesForType(type);
			for (String tMatch : typeMatches) {
				if (!matches.contains(tMatch))
					matches.add(tMatch);
			}
		}
		return matches;
	}
	
	protected void addTokenPattern(String columnKey, Integer tokenCount, String tokenPattern) {
		String tokenId = generateFieldKey("WordToken", columnKey, tokenCount);
		tokenPatterns.put(tokenId, tokenPattern);
	}
	
	protected String generateFieldKey(String type, String columnKey, Integer fieldCount) {
		String fieldKey = typeToId(type);
		if (fieldCount == -1 && columnKey != null && !type.equals("Corpus") && !type.equals("Collection") && !type.equals("Document"))
			fieldKey = typeToId(type)+columnKey;
		else if (fieldCount > -1)
			fieldKey = typeToId(type)+columnKey+alphabet[fieldCount];
		
		String[] a = new String[]{"WordType", "Lemma", "PosTag", "PosHead", "PosFeature", "Phonetic"};
		if (Arrays.asList(a).contains(type)) {
			if (!fieldKeysPerColumn.containsKey(columnKey))
				fieldKeysPerColumn.put(columnKey, new ArrayList<String>());
			if (!fieldKeysPerColumn.get(columnKey).contains(fieldKey))
				fieldKeysPerColumn.get(columnKey).add(fieldKey);
		}
		return fieldKey;
	}

	protected String getCypherOperatorForField(JSONObject field) {
		System.out.println("getCypherOperatorForField("+field.toString()+")");
		String operator = "equal";
		if (field.has("operator"))
			operator = field.getString("operator");
		String pattern = field.getString("pattern");
		pattern = pattern.replaceAll("\\\\[\\.\\*\\{\\}\\[\\]\\(\\)]", "");
		Pattern nonAlphaNumeric = Pattern.compile(".*\\W+.*");
		Matcher matcher = nonAlphaNumeric.matcher(pattern);
		if (matcher.matches() && operator.equals("equal")) {
			return "=~";
		} else if (matcher.matches() && operator.equals("not_equal")) {
			return "!~";
		} else if (operator.equals("not_equal")) {
			return "<>";
		}
		return "=~";
	}

	protected String groupToType(String group) {
		if (group.contains("lemma"))
			return "Lemma";
		else if (group.contains("pos"))
			return "PosTag";
		else if (group.contains("phonetic"))
			return "Phonetic";
		return "WordType";
	}
	
	protected String typeToId(String type) {
		if (type.equals("WordType"))
			return "w";
		else if (type.equals("Lemma"))
			return "l";
		else if (type.equals("PosTag"))
			return "p";
		else if (type.equals("PosHead"))
			return "pd";
		else if (type.equals("PosFeature"))
			return "pf";
		else if (type.equals("Phonetic"))
			return "ph";
		else if (type.equals("Metadatum"))
			return "m";
		else if (type.equals("Corpus"))
			return "corpus";
		else if (type.equals("Collection"))
			return "collection";
		else if (type.equals("Document"))
			return "document";
		return "t";
	}
	
	protected String typeToRelationship(String type) {
		if (type.equals("WordType"))
			return ":HAS_TYPE";
		else if (type.equals("Lemma"))
			return ":HAS_LEMMA";
		else if (type.equals("PosTag"))
			return ":HAS_POS_TAG";
		else if (type.equals("PosHead"))
			return ":HAS_HEAD";
		else if (type.equals("PosFeature"))
			return ":HAS_FEATURE";
		else if (type.equals("Phonetic"))
			return ":HAS_PHONETIC";
		return "";
	}
	
	protected String typeToParameter(String type) {
		if (type.equals("Lemma"))
			return "lemma";
		else if (type.equals("PosTag"))
			return "pos";
		else if (type.equals("Phonetic"))
			return "phonetic";
		return "text";
	}
	
	private Integer getColumnQueryType(JSONObject column) {
		System.out.println("getColumnQueryType");
		System.out.println(column.toString());
		if (column.has("repeat_of"))
			return 0; // repeat query
		else if (column.has("fields")) {
			JSONArray fields = (JSONArray) column.get("fields");
			if (fields.length() == 1 && !fields.getJSONObject(0).has("fields"))
				return 1; // single field query, single value
			else {
				List<String> types = new ArrayList<String>();
				for (int i = 0; i < fields.length(); i++) {
					JSONObject field = fields.getJSONObject(i);
					if (field.has("fields"))
						return 4; // nested query
					else {
						String type = field.getString("field");
						if (!types.contains(type))
							types.add(type);
					}
				}
				if (types.size() == 1)
					return 2; // single field query, multiple values
				else
					return 3; // multi field query
				
			}
		}
		return -1; // unknown query type
	}
	
	protected Map<String, String> parseMetadataFieldString(String field) {
		Map<String, String> metadatum = new HashMap<String, String>();
		Pattern pattern = Pattern.compile("([A-Za-z0-9]+)_([A-Za-z0-9_]+)(\\!*=|\\>=*|\\<=*)(.+)");
		Matcher matcher = pattern.matcher(field);
		if (matcher.matches()) {
			metadatum.put("group", matcher.group(1));
			metadatum.put("key", matcher.group(2));
			String operator = matcher.group(3);
			if (operator.equals("!="))
				operator = "<>";
			String value = matcher.group(4);
			if (value.indexOf('"') == 0)
				value = value.substring(1,value.length()-1);
			metadatum.put("operator", operator);
			metadatum.put("value", value);
		}
		return metadatum;
	}
	
	private Map<Long, List<String>> reorderColumnKeys(JSONObject columns) {
		// sort columns in ascending order on "highest_frequency"
		Map<Long, List<String>> sortedColumns = new HashMap<Long, List<String>>();
		List<String> columnsWithoutFrequency = new ArrayList<String>();
		Long maxHighestFrequency = (long) 0;
		Iterator<String> columnIt = columns.keys();
		while (columnIt.hasNext()) {
			String columnKey = columnIt.next();
			JSONObject column = columns.getJSONObject(columnKey);
			if (column.has("highest_frequency")) {
				Long highestFrequency = column.getLong("highest_frequency");
				if (highestFrequency > maxHighestFrequency)
					maxHighestFrequency = highestFrequency;
				if (!sortedColumns.containsKey(highestFrequency))
					sortedColumns.put(highestFrequency, new ArrayList<String>());
				sortedColumns.get(highestFrequency).add(columnKey);
			} else
				columnsWithoutFrequency.add(columnKey);
		}
		for (String columnKey : columnsWithoutFrequency) {
			long h = maxHighestFrequency + (columns.length() - Integer.valueOf(columnKey));
			if (!sortedColumns.containsKey(h))
				sortedColumns.put(h, new ArrayList<String>());
			sortedColumns.get(h).add(columnKey);
		}
		return sortedColumns;
	}
	
	private JSONArray reorderFields(JSONArray fields) {
		JSONArray sorted = new JSONArray();
		Map<Long, List<JSONObject>> byFreq = new HashMap<Long, List<JSONObject>>();
		List<JSONObject> noFreq = new ArrayList<JSONObject>();
		
		for (int f = 0; f < fields.length(); f++) {
			JSONObject field = fields.getJSONObject(f);
			if (field.has("frequency")) {
				Long freq = field.getLong("frequency");
				if (!byFreq.containsKey(freq))
					byFreq.put(freq, new ArrayList<JSONObject>());
				byFreq.get(freq).add(field);
			} else if (field.has("fields")) {
				field.put("fields", reorderFields(field.getJSONArray("fields")));
				Long freq = field.getLong("highest_frequency");
				if (!byFreq.containsKey(freq))
					byFreq.put(freq, new ArrayList<JSONObject>());
				byFreq.get(freq).add(field);
			} else {
				noFreq.add(field);
			}
		}
		
		SortedSet<Long> keys = new TreeSet<Long>(byFreq.keySet());
		for (Long freq : keys) {
			for (JSONObject field : byFreq.get(freq)) {
				sorted.put(field);
			}
		}
		for (JSONObject field : noFreq) {
			sorted.put(field);
		}
		
		return sorted;
	}
	
	
	////////////////////////////////////////////////////////////////
	
	protected void beforeTemplate(Query q) {
		query = q;
		hasEmptyPattern = query.hasEmptyPattern();
		hasEmptyFilter = query.hasEmptyFilter();
		if (hasEmptyPattern && query.getView() != 20)
			tokenIds.add("t1");
		addStartNodes();
		addMetadataFilter();
		if (query.getView() != 20)
			addTokenPatterns();
	}
	
	protected void addStartNodes() {
		System.out.println("addStartNodes");
		if (!hasEmptyPattern) {
			JSONObject columns = query.toJSON();
			Map<Long, List<String>> sortedColumns = reorderColumnKeys(columns);
			SortedSet<Long> keys = new TreeSet<Long>(sortedColumns.keySet());
			for (Long frequency : keys) {
				for (String columnKey : sortedColumns.get(frequency)) {
					System.out.println("columnKey: "+columnKey);
					Integer fieldCount = 0;
					JSONObject column = columns.getJSONObject(columnKey);
					addColumnStartNodes(column, columnKey, fieldCount);
				}
			}
		}
		if (!hasEmptyFilter) {
			String filter = query.getFilter();
			String[] fields = filter.substring(1, filter.length()-1).split("\\)AND\\(");
			List<String> fieldsAdded = new ArrayList<String>();
			for (int f = 0; f < fields.length; f++) {
				Map<String, String> metadatum = parseMetadataFieldString(fields[f]);
				if (metadatum.get("group").equals("Corpus") || metadatum.get("group").equals("Collection"))
					addStartNode(metadatum.get("group"), "", f, metadatum.get("key")+":"+metadatum.get("value"));
				else if (!fieldsAdded.contains(metadatum.get("key")+":"+metadatum.get("value"))) {
					addStartNode("Metadatum", "", f, "group:"+metadatum.get("group")+" AND key:"+metadatum.get("key"));
					fieldsAdded.add(metadatum.get("key")+":"+metadatum.get("value"));
				}
			}
		}
//		if (query.getDocpid() != null) {
//			addStartNode("Document", "", -1, "xmlid:"+query.getDocpid());
//		}
	}
	
	private Integer addColumnStartNodes(JSONObject column, String columnKey, Integer fieldCount) {
		Integer queryType = getColumnQueryType(column);
		
		if (queryType > 0) {
			if (queryType == 1 || queryType == 2) {
				fieldCount = getSingleFieldQuery(column, columnKey, fieldCount);
			} else {
				fieldCount = getMultiFieldQuery(column, columnKey, fieldCount);
			}
		}
		return fieldCount;
	}
	
	private Integer getSingleFieldQuery(JSONObject column, String columnKey, Integer fieldCount) {
		System.out.println("getSingleFieldQuery");
		JSONArray fields = (JSONArray) column.get("fields");
		if (fields.length() > 1)
			fields = reorderFields(fields);
		String type = null;
		List<String> patterns = new ArrayList<String>();
		for (int i = 0; i < fields.length(); i++) {
			JSONObject field = fields.getJSONObject(i);
			if (type == null)
				type = field.getString("field");
			String pattern = field.getString("pattern");
			if (!pattern.equals(".*")) {
				pattern = pattern.replaceAll("\\*", "\\\\\\\\*").replaceAll("\\(", "\\\\\\\\(").replaceAll("\\)", "\\\\\\\\)").replaceAll("\\-", "\\\\\\\\-").replaceAll("\\.", "\\\\\\\\.");
				patterns.add("label:"+pattern);
			}
		}
		String connector = "OR";
		if (column.has("connector"))
			connector = column.getString("connector");
		
		if (patterns.size() > 0)
			addStartNode(type, columnKey, fieldCount, StringUtils.join(patterns.toArray()," "+connector+" "));
		
		fieldCount++;
		return fieldCount;
	}
	
	private Integer getMultiFieldQuery(JSONObject column, String columnKey, Integer fieldCount) {
		JSONArray fields = (JSONArray) column.get("fields");
		if (fields.length() > 1)
			fields = reorderFields(fields);
		for (int i = 0; i < fields.length(); i++) {
			JSONObject field = fields.getJSONObject(i);
			if (field.has("field")) {
				if (!field.has("repeat_of")) {
					String type = field.getString("field");
					String pattern = field.getString("pattern");
					if (!pattern.equals(".*")) {
						pattern = pattern.replaceAll("\\*", "\\\\\\\\*").replaceAll("\\(", "\\\\\\\\(").replaceAll("\\)", "\\\\\\\\)").replaceAll("\\-", "\\\\\\\\-").replaceAll("\\.", "\\\\\\\\.");
						addStartNode(type, columnKey, fieldCount, "label:"+pattern);
					}
					fieldCount++;
				}
			} else if (field.has("fields")) {
				fieldCount = addColumnStartNodes(field, columnKey, fieldCount);
			}
		}
		return fieldCount;
	}
	
	private void addTokenPatterns() {
		if (hasEmptyPattern) {
			if (hasEmptyFilter)
				tokenPattern = " MATCH (document:Document)-[:HAS_TOKEN]->(t1:WordToken) ";
			else if (query.getView() != 20)
				tokenPattern = " MATCH (document)-[:HAS_TOKEN]->(t1:WordToken) ";
		} else {
			JSONObject columns = query.toJSON();
			Map<Long, List<String>> sortedColumns = reorderColumnKeys(columns);
			SortedSet<Long> keys = new TreeSet<Long>(sortedColumns.keySet());
			for (Long frequency : keys) {
				if (sortedColumns.get(frequency).contains("1") && !sortedColumns.get(frequency).contains(String.valueOf(query.getColumnCount())))
					Collections.sort(sortedColumns.get(frequency), Collections.reverseOrder());
				
				for (String columnKey : sortedColumns.get(frequency)) {
					JSONObject column = columns.getJSONObject(columnKey);
					tokenCount = 0;
					columnTokenIds = new ArrayList<String>();
					getComplexFieldTokenPattern(column, columnKey, 0);
					if (columnTokenIds.size() > 0) {
						tokenPattern = tokenPattern + " WITH DISTINCT document, "+getCaseWhenForToken(columnTokenIds)+" AS t"+columnKey+" ";
					}
					
					List<String> remainingNodes = getStartNodesForTypes(new String[]{"Corpus", "Collection", "Metadatum"});
					if (remainingNodes.size() > 0)
						tokenPattern = tokenPattern + ", " + StringUtils.join(remainingNodes.toArray(), ", ") + " ";
					
					closeTokenColumn(columnKey);
					tokenIds.add("t"+columnKey);
				}
			}
		}
	}
	
	private String getCaseWhenForToken(List<String> tokens) {
		String firstTokenId = tokens.remove(0);
		if (tokens.size() == 0) {
			return firstTokenId;
		} else {
			return "CASE WHEN "+firstTokenId+" IS NULL THEN "+getCaseWhenForToken(tokens)+" ELSE "+firstTokenId+" END";
		}
	}
	
	private Integer getComplexFieldTokenPattern(JSONObject field, String columnKey, int fieldCount) {
		if (field.has("fields")) {
			JSONArray fields = field.getJSONArray("fields");
			int totalFieldCount = fields.length();
			String operator = "AND";
			if (field.has("operator"))
				operator = field.getString("operator");
			
			for (int i = 0; i < totalFieldCount; i++) {
				fieldCount = getFieldTokenPattern(fields.getJSONObject(i), columnKey, fieldCount);
				if (operator.equals("OR"))
					tokenCount++;
			}
		} else if (field.has("repeat_of")) {
			return getRepeatFieldTokenPattern(field, columnKey, fieldCount);
		}
		return fieldCount;
	}
	
	private void closeTokenColumn(String columnKey) {
		List<String> contentNodes = getStartNodesForTypes(new String[]{"WordType", "Lemma", "PosTag", "PosHead", "Phonetic"});
		
		if (tokenIds.size() > 0) {
			tokenPattern = tokenPattern + ", " + StringUtils.join(tokenIds.toArray(), ", ")+" ";
		}
		if (fieldKeysPerColumn.containsKey(columnKey)) {
			List<String> fieldKeys = fieldKeysPerColumn.get(columnKey);
			for (String fieldKey : fieldKeys) {
				if (contentNodes.contains(fieldKey) && !processedKeys.contains(fieldKey)) {
					tokenPattern = tokenPattern + ", " + fieldKey;
					processedKeys.add(fieldKey);
				}
			}
		}
		int columnCount = query.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			String cKey = String.valueOf(i);
			if (fieldKeysPerColumn.containsKey(cKey)) {
				List<String> fieldKeys = fieldKeysPerColumn.get(cKey);
//				tokenPattern = tokenPattern + ", " + StringUtils.join(fieldKeys.toArray(), ", ")+" ";
				for (String fieldKey : fieldKeys) {
					if (contentNodes.contains(fieldKey) && !processedKeys.contains(fieldKey)) {
						tokenPattern = tokenPattern + ", " + fieldKey;
						processedKeys.add(fieldKey);
					}
				}
			}
		}
//		for (String fieldKey : getStartNodesForType("Corpus")) {
//			tokenPattern = tokenPattern + ", " + fieldKey;
//		}
//		for (String fieldKey : getStartNodesForType("Collection")) {
//			tokenPattern = tokenPattern + ", " + fieldKey;
//		}
//		for (String fieldKey : getStartNodesForType("Metadatum")) {
//			tokenPattern = tokenPattern + ", " + fieldKey;
//		}
	}
	
	private Integer getFieldTokenPattern(JSONObject field, String columnKey, int fieldCount) {
		if (field.has("repeat_of") || field.has("fields"))
			return getComplexFieldTokenPattern(field, columnKey, fieldCount);
		else
			return getSingleFieldTokenPattern(field, columnKey, fieldCount);
	}
	
	private Integer getRepeatFieldTokenPattern(JSONObject field, String columnKey, int fieldCount) {
		Integer repeatOf = field.getInt("repeat_of");
		String tokenId = generateFieldKey("WordToken", columnKey, -1);
		String prevTokenId = "t"+String.valueOf(Integer.valueOf(columnKey) - 1);
		String nextTokenId = "t"+String.valueOf(Integer.valueOf(columnKey) + 1);
		if (!columnTokenIds.contains(tokenId))
			columnTokenIds.add(tokenId);
		if (field.has("field")) {
			String fieldType = field.getString("field");
			if ((tokenIds.contains(prevTokenId) || tokenIds.contains(nextTokenId)) && !connectedTokenIds.contains(tokenId)) {
				if (tokenIds.contains(prevTokenId) && tokenIds.contains(nextTokenId))
					tokenPattern = tokenPattern + " MATCH ("+prevTokenId+")-[:NEXT]->("+tokenId+":WordToken)-[:NEXT]->("+nextTokenId+") "
							+ " MATCH ("+tokenId+")-["+typeToRelationship(fieldType)+"]->(:"+fieldType+")<-["+typeToRelationship(fieldType)+"]-(t"+repeatOf+")";
				else if (tokenIds.contains(prevTokenId))
					tokenPattern = tokenPattern + " MATCH ("+prevTokenId+")-[:NEXT]->("+tokenId+":WordToken) "
						+ " MATCH ("+tokenId+")-["+typeToRelationship(fieldType)+"]->(:"+fieldType+")<-["+typeToRelationship(fieldType)+"]-(t"+repeatOf+")";
				else if (tokenIds.contains(nextTokenId))
					tokenPattern = tokenPattern + " MATCH ("+tokenId+":WordToken)-[:NEXT]->("+nextTokenId+") "
						+ " MATCH ("+tokenId+")-["+typeToRelationship(fieldType)+"]->(:"+fieldType+")<-["+typeToRelationship(fieldType)+"]-(t"+repeatOf+")";
			} else
				tokenPattern = tokenPattern + " MATCH ("+tokenId+":WordToken)-["+typeToRelationship(fieldType)+"]->(:"+fieldType+")<-["+typeToRelationship(fieldType)+"]-(t"+repeatOf+") ";
		} else {
			if ((tokenIds.contains(prevTokenId) || tokenIds.contains(nextTokenId)) && !connectedTokenIds.contains(tokenId)) {
				if (tokenIds.contains(prevTokenId) && tokenIds.contains(nextTokenId))
					tokenPattern = tokenPattern + " MATCH ("+prevTokenId+")-[:NEXT]->("+tokenId+":WordToken)-[:NEXT]->("+tokenIds.contains(nextTokenId)+") "
							+ " MATCH ("+tokenId+")-[:HAS_TYPE]->(:WordType)<-[:HAS_TYPE]-(t"+repeatOf+") "
							+  " MATCH ("+tokenId+")-[:HAS_LEMMA]->(:Lemma)<-[:HAS_LEMMA]-(t"+repeatOf+") "
							+  " MATCH ("+tokenId+")-[:HAS_LEMMA]->(:Lemma)<-[:HAS_LEMMA]-(t"+repeatOf+") ";
				else if (tokenIds.contains(prevTokenId))
					tokenPattern = tokenPattern + " MATCH ("+prevTokenId+")-[:NEXT]->("+tokenId+":WordToken) "
						+ " MATCH ("+tokenId+")-[:HAS_TYPE]->(:WordType)<-[:HAS_TYPE]-(t"+repeatOf+") "
						+  " MATCH ("+tokenId+")-[:HAS_LEMMA]->(:Lemma)<-[:HAS_LEMMA]-(t"+repeatOf+") "
						+  " MATCH ("+tokenId+")-[:HAS_LEMMA]->(:Lemma)<-[:HAS_LEMMA]-(t"+repeatOf+") ";
				else if (tokenIds.contains(nextTokenId))
					tokenPattern = tokenPattern + " MATCH ("+tokenId+":WordToken)-[:NEXT]->("+tokenIds.contains(nextTokenId)+") "
						+ " MATCH ("+tokenId+")-[:HAS_TYPE]->(:WordType)<-[:HAS_TYPE]-(t"+repeatOf+") "
						+  " MATCH ("+tokenId+")-[:HAS_LEMMA]->(:Lemma)<-[:HAS_LEMMA]-(t"+repeatOf+") "
						+  " MATCH ("+tokenId+")-[:HAS_LEMMA]->(:Lemma)<-[:HAS_LEMMA]-(t"+repeatOf+") ";
			} else
				tokenPattern = tokenPattern + " MATCH ("+tokenId+":WordToken)-[:HAS_TYPE]->(:WordType)<-[:HAS_TYPE]-(t"+repeatOf+") "
						+  " MATCH ("+tokenId+")-[:HAS_LEMMA]->(:Lemma)<-[:HAS_LEMMA]-(t"+repeatOf+") "
						+  " MATCH ("+tokenId+")-[:HAS_LEMMA]->(:Lemma)<-[:HAS_LEMMA]-(t"+repeatOf+") ";
		}
//		fieldCount++;
		return fieldCount;
	}
	
	private Integer getSingleFieldTokenPattern(JSONObject field, String columnKey, int fieldCount) {
		String fieldType = field.getString("field");
		String fieldKey = generateFieldKey(fieldType, columnKey,fieldCount);
		String tokenId = generateFieldKey("WordToken", columnKey, tokenCount);
		String prevTokenId = "t"+String.valueOf(Integer.valueOf(columnKey) - 1);
		String nextTokenId = "t"+String.valueOf(Integer.valueOf(columnKey) + 1);
		String tokenStartNode = tokenId;
		if (!columnTokenIds.contains(tokenId)) {
			columnTokenIds.add(tokenId);
			if ((tokenIds.contains(prevTokenId) || tokenIds.contains(nextTokenId)) && !connectedTokenIds.contains(tokenId)) {
				if (tokenIds.contains(prevTokenId) && tokenIds.contains(nextTokenId))
					tokenStartNode = "("+prevTokenId+")-[:NEXT]->("+tokenId+":WordToken)-[:NEXT]->("+nextTokenId+")";
				else if (tokenIds.contains(prevTokenId))
					tokenStartNode = "("+prevTokenId+")-[:NEXT]->("+tokenId+":WordToken)";
				else if (tokenIds.contains(nextTokenId))
					tokenStartNode = "("+tokenId+":WordToken)-[:NEXT]->("+nextTokenId+")";
			} else
				tokenStartNode = "("+tokenId+":WordToken)";
		}
		
		if (!tokenStartNode.startsWith("("))
				tokenStartNode = "("+tokenStartNode+")";
		
		if (!documentNodeAdded) {
			if (hasEmptyFilter && query.getDocpid() != null && query.getDocpid().length() > 0)
				tokenPattern = " MATCH (document:Document{xmlid:'"+query.getDocpid()+"'})-[:HAS_TOKEN]->"+tokenStartNode;
			else if (hasEmptyFilter)
				tokenPattern = tokenPattern + " MATCH (document:Document)-[:HAS_TOKEN]->"+tokenStartNode;
			else
				tokenPattern = tokenPattern + " MATCH (document)-[:HAS_TOKEN]->"+tokenStartNode;
			documentNodeAdded = true;
		} else
			tokenPattern = tokenPattern + " MATCH "+tokenStartNode;
		
		if (startNodes.keySet().contains(fieldKey)) {
			if (fieldType.equals("PosHead") || fieldType.equals("PosFeature"))
				tokenPattern = tokenPattern + "-["+typeToRelationship("PosTag")+"]->(:PosTag)-["+typeToRelationship(fieldType)+"]->("+fieldKey+")";
			else
				tokenPattern = tokenPattern + "-["+typeToRelationship(fieldType)+"]->("+fieldKey+")";
		}
		
		Boolean check = false;
		if (query.getWithin().equals("paragraph") && Integer.valueOf(columnKey) > 1) {
			tokenPattern = tokenPattern + " WHERE NOT ("+tokenId+")<-[:STARTS_AT]-(:ParagraphStart) ";
			check = true;
		} else if (query.getWithin().equals("sentence") && Integer.valueOf(columnKey) > 1) {
			tokenPattern = tokenPattern + " WHERE NOT ("+tokenId+")<-[:STARTS_AT]-(:Sentence) ";
			check = true;
		}
		
		Boolean sensitive = false;
		if (field.has("case_sensitive"))
			sensitive = field.getBoolean("case_sensitive");
		
		if (sensitive) {
			if (check)
				tokenPattern = tokenPattern + " AND "+fieldKey+".label = '"+field.getString("pattern")+"' ";
			else
				tokenPattern = tokenPattern + " WHERE "+fieldKey+".label = '"+field.getString("pattern")+"' ";
		}

		if (startNodes.keySet().contains(fieldKey) && !processedKeys.contains(fieldKey))
			processedKeys.add(fieldKey);
		
		fieldCount++;
		return fieldCount;
	}
	
	@SuppressWarnings("unchecked")
	protected void addMetadataFilter() {
		if (hasEmptyFilter && !hasEmptyPattern) {
// 			if (query.getView() != 20) {
// 				if (query.getDocpid() != null && query.getDocpid().length() > 0)
// 					metadataFilter = " MATCH (document:Document{xmlid:'"+query.getDocpid()+"'})";
// 				else
// 					metadataFilter = " MATCH (document:Document)";
// 			}
		} else if (!hasEmptyFilter) {
			String filter = query.getFilter();
			metadataFilter = "";
			String[] fields = filter.substring(1, filter.length()-1).split("\\)AND\\(");
			Map<String,Map<String, Object>> metaFilters = new HashMap<String,Map<String, Object>>();
			Map<String,Map<String, Object>> notMetaFilters = new HashMap<String,Map<String, Object>>();
			List<String> corpora = new ArrayList<String>();
			List<String> collections = new ArrayList<String>();
			List<String> notCorpora = new ArrayList<String>();
			List<String> notCollections = new ArrayList<String>();
//			List<String> metadataNodes = getStartNodesForType("Metadatum");
//			List<String> contentNodes = getStartNodesForTypes(new String[]{"WordType", "Lemma", "PosTag", "PosHead", "Phonetic"});
			
			for (int f = 0; f < fields.length; f++) {
				System.out.println("Metadata field: "+fields[f]);
				Map<String, String> metadatum = parseMetadataFieldString(fields[f]);
				
				if (metadatum.get("group").equals("Corpus") && metadatum.get("key").equals("title")) {
					if (metadatum.containsKey("operator") && metadatum.get("operator").equals("!="))
						notCorpora.add(getStartNode("Corpus", "", f));
					else
						corpora.add(getStartNode("Corpus", "", f));
				} else if (metadatum.get("group").equals("Collection") && metadatum.get("key").equals("title")) {
					if (metadatum.containsKey("operator") && metadatum.get("operator").equals("!="))
						notCollections.add(getStartNode("Collection", "", f));
					else
						collections.add(getStartNode("Collection", "", f));
				} else if (metadatum.get("operator").equals("!=")) {
					if (!notMetaFilters.containsKey(metadatum.get("group")+"_"+metadatum.get("key"))) {
						Map<String, Object> data = new HashMap<String, Object>();
						data.put("group", metadatum.get("group"));
						data.put("key", metadatum.get("key"));
						data.put("values", new ArrayList<String>());
						data.put("nodeId", getStartNode("Metadatum", "", f));
						notMetaFilters.put(metadatum.get("group")+"_"+metadatum.get("key"),data);
					}
					((List<String>) notMetaFilters.get(metadatum.get("group")+"_"+metadatum.get("key")).get("values")).add(metadatum.get("value"));
				} else {
					if (!metaFilters.containsKey(metadatum.get("group")+"_"+metadatum.get("key"))) {
						Map<String, Object> data = new HashMap<String, Object>();
						data.put("group", metadatum.get("group"));
						data.put("key", metadatum.get("key"));
						data.put("values", new ArrayList<String>());
						data.put("nodeId", getStartNode("Metadatum", "", f));
						metaFilters.put(metadatum.get("group")+"_"+metadatum.get("key"),data);
					}
					((List<String>) metaFilters.get(metadatum.get("group")+"_"+metadatum.get("key")).get("values")).add(metadatum.get("value"));
				}
			}
			
			if (corpora.size() > 0) {
				if (corpora.size() == 1) {
					metadataFilter = metadataFilter + " MATCH ("+corpora.get(0)+")";
					if (collections.size() > 0) {
						if (collections.size() == 1) {
							metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->("+collections.get(0)+") ";
						} else
							metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) "
								+ "WHERE collection IN ["+StringUtils.join(collections.toArray(), ",")+"] ";
					} else if (query.getGroup() != null && query.getGroup().equals("Collection_title")) {
						metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) ";
					}
					if (notCollections.size() > 0) {
						if (collections.size() > 1)
							metadataFilter = metadataFilter + "AND collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else if (collections.size() == 1)
							metadataFilter = metadataFilter + "WHERE "+collections.get(0)+" NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else if (query.getGroup() != null && query.getGroup().equals("Collection_title"))
							metadataFilter = metadataFilter + "WHERE collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else
							metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) WHERE collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
					}
					if (notCorpora.size() > 0) {
						if (collections.size() > 1 || notCollections.size() > 0) // INSIDE WHERE
							metadataFilter = metadataFilter + "AND "+corpora.get(0)+" NOT IN ["+StringUtils.join(notCorpora.toArray(), ",")+"] ";
						else
							metadataFilter = metadataFilter + " WHERE "+corpora.get(0)+" NOT IN ["+StringUtils.join(notCorpora.toArray(), ",")+"] ";
					}
//					if (notCollections.size() == 0 && notCorpora.size() == 0) {
//						metadataFilter = metadataFilter + " ";
//					}
				} else {
					metadataFilter = metadataFilter + " MATCH (corpus:Corpus)";
					if (collections.size() > 0) {
						if (collections.size() == 1)
							metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->("+collections.get(0)+") ";
						else
							metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) "
								+ "WHERE collection.title IN ["+StringUtils.join(collections.toArray(), ",")+"] ";
					} else if (query.getGroup() != null && query.getGroup().equals("Collection_title")) {
						metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) ";
					}
					if (notCollections.size() > 0) {
						if (collections.size() > 1)
							metadataFilter = metadataFilter + "AND collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else if (collections.size() == 1)
							metadataFilter = metadataFilter + "WHERE "+collections.get(0)+" NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else if (query.getGroup() != null && query.getGroup().equals("Collection_title"))
							metadataFilter = metadataFilter + "WHERE collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else
							metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) WHERE collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
					}
					if (collections.size() > 1 || notCollections.size() > 0) // INSIDE WHERE
						metadataFilter = metadataFilter + "AND corpus IN ["+StringUtils.join(corpora.toArray(), ",")+"] ";
					else
						metadataFilter = metadataFilter + "WHERE corpus IN ["+StringUtils.join(corpora.toArray(), ",")+"] ";
					if (notCorpora.size() > 0) {
						metadataFilter = metadataFilter + "AND corpus NOT IN ["+StringUtils.join(notCorpora.toArray(), ",")+"] ";
					}
				}
			} else if (query.getGroup() != null && query.getGroup().equals("Corpus_title")) {
				metadataFilter = metadataFilter + " MATCH (corpus:Corpus)";
				if (collections.size() > 0) {
					if (collections.size() == 1)
						metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->("+collections.get(0)+") ";
					else
						metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) "
							+ "WHERE collection.title IN ["+StringUtils.join(collections.toArray(), ",")+"] ";
				}
				if (notCollections.size() > 0) {
					if (collections.size() > 1)
						metadataFilter = metadataFilter + "AND collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
					else if (collections.size() == 1)
						metadataFilter = metadataFilter + "WHERE "+collections.get(0)+" NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
					else
						metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) WHERE collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
				}
				if (notCorpora.size() > 0) {
					if (collections.size() > 1 || notCollections.size() > 0) // INSIDE WHERE
						metadataFilter = metadataFilter + "AND corpus NOT IN ["+StringUtils.join(notCorpora.toArray(), ",")+"] ";
					else
						metadataFilter = metadataFilter + "WHERE corpus NOT IN ["+StringUtils.join(notCorpora.toArray(), ",")+"] ";
				}
			} else {
				if (notCorpora.size() > 0) {
					metadataFilter = metadataFilter + " MATCH (corpus:Corpus)";
					if (collections.size() > 0) {
						if (collections.size() == 1)
							metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->("+collections.get(0)+") ";
						else
							metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) "
								+ "WHERE collection.title IN ["+StringUtils.join(collections.toArray(), ",")+"] ";
					} else if (query.getGroup() != null && query.getGroup().equals("Collection_title")) {
						metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) ";
					}
					if (notCollections.size() > 0) {
						if (collections.size() > 1)
							metadataFilter = metadataFilter + "AND collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else if (collections.size() == 1)
							metadataFilter = metadataFilter + "WHERE "+collections.get(0)+" NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else if (query.getGroup() != null && query.getGroup().equals("Collection_title"))
							metadataFilter = metadataFilter + "WHERE collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else
							metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(collection:Collection) WHERE collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
					}
					if (collections.size() > 1 || notCollections.size() > 0) // INSIDE WHERE
						metadataFilter = metadataFilter + "AND corpus NOT IN ["+StringUtils.join(notCorpora.toArray(), ",")+"] ";
					else
						metadataFilter = metadataFilter + "WHERE corpus NOT IN ["+StringUtils.join(notCorpora.toArray(), ",")+"] ";
				} else {
					if (collections.size() > 0) {
						if (collections.size() == 1)
							metadataFilter = metadataFilter + " MATCH ("+collections.get(0)+") ";
						else
							metadataFilter = metadataFilter + " MATCH (collection:Collection) "
								+ "WHERE collection.title IN ["+StringUtils.join(collections.toArray(), ",")+"] ";
					} else if (query.getGroup() != null && query.getGroup().equals("Collection_title")) {
						metadataFilter = metadataFilter + " MATCH (collection:Collection) ";
					}
					if (notCollections.size() > 0) {
						if (collections.size() > 1)
							metadataFilter = metadataFilter + "AND collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else if (collections.size() == 1)
							metadataFilter = metadataFilter + "WHERE "+collections.get(0)+" NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else if (query.getGroup() != null && query.getGroup().equals("Collection_title"))
							metadataFilter = metadataFilter + "WHERE collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
						else
							metadataFilter = metadataFilter + " MATCH (collection:Collection) WHERE collection NOT IN ["+StringUtils.join(notCollections.toArray(), ",")+"] ";
					}
				}
			}
			
			String documentNode = "(document:Document)";
			if (query.getDocpid() != null && query.getDocpid().length() > 0)
				documentNode = "(document:Document{xmlid:'"+query.getDocpid()+"'})";
			
			if (collections.size() == 0 && notCollections.size() == 0 && (query.getGroup() == null || !query.getGroup().equals("Collection_title"))) {
				if (corpora.size() == 0 && notCorpora.size() == 0 && (query.getGroup() == null || !query.getGroup().equals("Corpus_title"))) {
					metadataFilter = metadataFilter + " MATCH "+documentNode;
				} else if (corpora.size() > 1 || notCorpora.size() > 0 || (query.getGroup() != null && query.getGroup().equals("Corpus_title"))) {
					metadataFilter = metadataFilter + " MATCH (corpus)-[:HAS_COLLECTION]->(:Collection)-[:HAS_DOCUMENT]->"+documentNode;
				} else if (corpora.size() == 1) {
					metadataFilter = metadataFilter + "-[:HAS_COLLECTION]->(:Collection)-[:HAS_DOCUMENT]->"+documentNode;
				}
			} else if (collections.size() > 1 || notCollections.size() > 0 || (query.getGroup() != null && query.getGroup().equals("Collection_title"))) {
				if (corpora.size() == 0 && notCorpora.size() == 0 && (query.getGroup() == null || !query.getGroup().equals("Corpus_title"))) {
					metadataFilter = metadataFilter + " MATCH (collection)-[:HAS_DOCUMENT]->"+documentNode;
				} else if (corpora.size() > 1 || notCorpora.size() > 0 || (query.getGroup() != null && query.getGroup().equals("Corpus_title"))) {
					metadataFilter = metadataFilter + " MATCH (corpus)-[:HAS_COLLECTION]->(collection)-[:HAS_DOCUMENT]->"+documentNode;
				} else if (corpora.size() == 1) {
					metadataFilter = metadataFilter + " MATCH ("+corpora.get(0)+")-[:HAS_COLLECTION]->(collection)-[:HAS_DOCUMENT]->"+documentNode;
				}
			} else if (collections.size() == 1) {
				if (corpora.size() == 0 && notCorpora.size() == 0 && (query.getGroup() == null || !query.getGroup().equals("Corpus_title"))) {
					metadataFilter = metadataFilter + " MATCH ("+collections.get(0)+")-[:HAS_DOCUMENT]->"+documentNode;
				} else if (corpora.size() > 1 || notCorpora.size() > 0 || (query.getGroup() != null && query.getGroup().equals("Corpus_title"))) {
					metadataFilter = metadataFilter + " MATCH (corpus)-[:HAS_COLLECTION]->("+collections.get(0)+")-[:HAS_DOCUMENT]->"+documentNode;
				} else if (corpora.size() == 1) {
					metadataFilter = metadataFilter + " MATCH ("+corpora.get(0)+")-[:HAS_COLLECTION]->("+collections.get(0)+")-[:HAS_DOCUMENT]->"+documentNode;
				}
			}
			
			int r = 0;
			for (String metaLabel : metaFilters.keySet()) {
				Map<String, Object> data = metaFilters.get(metaLabel);
				List<String> values = (List<String>) data.get("values");
				if (values.size() > 1) {
					r++;
					metadataFilter = metadataFilter + "-[r"+String.valueOf(r)+":HAS_METADATUM]->("+(String) data.get("nodeId")+") "
						+ "WHERE TOSTRING(r"+String.valueOf(r)+".value) IN ['"+StringUtils.join(values.toArray(), "','")+"'] ";
				} else {
					r++;
					metadataFilter = metadataFilter + "-[r"+String.valueOf(r)+":HAS_METADATUM]->("+(String) data.get("nodeId")+") "
						+ "WHERE TOSTRING(r"+String.valueOf(r)+".value) = '"+values.get(0)+"' ";
				}
				if (notMetaFilters.containsKey(metaLabel)) {
					Map<String, Object> notData = notMetaFilters.get(metaLabel);
					List<String> notValues = (List<String>) notData.get("values");
					if (notValues.size() > 1) {
						metadataFilter = metadataFilter + "AND NOT TOSTRING(r"+String.valueOf(r)+".value) IN ['"+StringUtils.join(notValues.toArray(), "','")+"'] ";
					} else {
						metadataFilter = metadataFilter + "AND NOT TOSTRING(r"+String.valueOf(r)+".value) = '"+notValues.get(0)+"' ";
					}
				}
			}
			
			for (String metaLabel : notMetaFilters.keySet()) {
				Map<String, Object> data = notMetaFilters.get(metaLabel);
				List<String> values = (List<String>) data.get("values");
				if (!metaFilters.containsKey(metaLabel)) {
					if (values.size() > 1) {
						r++;
						if (metaFilters.size() == 0)
							metadataFilter = metadataFilter + "-[r"+String.valueOf(r)+":HAS_METADATUM]->("+(String) data.get("nodeId")+") "
									+ "WHERE NOT TOSTRING(r"+String.valueOf(r)+".value) IN ['"+StringUtils.join(values.toArray(), "','")+"'] ";
						else
							metadataFilter = metadataFilter + " MATCH (document)-[r"+String.valueOf(r)+":HAS_METADATUM]->("+(String) data.get("nodeId")+") "
									+ "WHERE NOT TOSTRING(r"+String.valueOf(r)+".value) IN ['"+StringUtils.join(values.toArray(), "','")+"'] ";
					} else {
						r++;
						if (metaFilters.size() == 0)
							metadataFilter = metadataFilter + "-[r"+String.valueOf(r)+":HAS_METADATUM]->("+(String) data.get("nodeId")+") "
									+ "WHERE NOT TOSTRING(r"+String.valueOf(r)+".value) = '"+values.get(0)+"' ";
						else
							metadataFilter = metadataFilter + " MATCH (document)-[r"+String.valueOf(r)+":HAS_METADATUM]->("+(String) data.get("nodeId")+") "
									+ "WHERE NOT TOSTRING(r"+String.valueOf(r)+".value) = '"+values.get(0)+"' ";
					}
				}
			}
			
//			if (query.getView() != 20) {
//				if (hasEmptyPattern)
//					metadataFilter = metadataFilter + " MATCH document-[:HAS_TOKEN]->(t1:WordToken) ";
//				else
//					metadataFilter = metadataFilter + " MATCH document-[:HAS_TOKEN]->t1 ";
//			}
		}
	}
	
	protected void addHitContent(String[] groups) {
		hitContent = "";
		for (int t = 1; t <= tokenIds.size(); t++) {
			String tokenId = "t"+String.valueOf(t);
			if (groups == null) {
				hitContent = hitContent + " MATCH ("+tokenId+")-[:HAS_TYPE]->(w"+String.valueOf(t)+":WordType) "
						+ " MATCH ("+tokenId+")-[:HAS_LEMMA]->(l"+String.valueOf(t)+":Lemma) "
						+ " MATCH ("+tokenId+")-[:HAS_POS_TAG]->(p"+String.valueOf(t)+":PosTag) "
						+ " MATCH ("+tokenId+")-[:HAS_PHONETIC]->(ph"+String.valueOf(t)+":Phonetic) ";
				
				if (t == 1)
					hitContent = hitContent + "WITH document, "+tokenId+".token_index AS first_index, "+tokenId+".begin_time AS begin_time, "
						+ "w"+String.valueOf(t)+".label AS hit_text, l"+String.valueOf(t)+".label AS hit_lemma, p"+String.valueOf(t)+".label AS hit_pos, "
						+ "ph"+String.valueOf(t)+".label AS hit_phonetic , "+StringUtils.join(tokenIds.toArray(), ", ")+" ";
				else
					hitContent = hitContent + "WITH document, first_index, begin_time, "
						+ "hit_text + ' ' + w"+String.valueOf(t)+".label AS hit_text, hit_lemma + ' ' + l"+String.valueOf(t)+".label AS hit_lemma, hit_pos + ' ' + p"+String.valueOf(t)+".label AS hit_pos, "
						+ "hit_phonetic + ' ' + ph"+String.valueOf(t)+".label AS hit_phonetic , "+StringUtils.join(tokenIds.toArray(), ", ")+" ";
				
				if (t == tokenIds.size())
					hitContent = hitContent + ", " + tokenId+".token_index AS last_index, "+tokenId+".end_time AS end_time ";
			} else {
				for (int g = 0; g < groups.length; g++) {
					String type = groupToType(groups[g]);
					hitContent = hitContent + " MATCH ("+tokenId+")-["+typeToRelationship(type)+"]->("+typeToId(type)+String.valueOf(t)+":"+type+") ";
				}
				hitContent = hitContent + "WITH document";
				
				for (int g = 0; g < groups.length; g++) {
					String type = groupToType(groups[g]);
					if (t == 1)
						hitContent = hitContent + ", "+typeToId(type)+String.valueOf(t)+".label AS "+groups[g];
					else
						hitContent = hitContent + ", "+groups[g]+" + ' ' + "+typeToId(type)+String.valueOf(t)+".label AS "+groups[g];
				}
				hitContent = hitContent + ", "+StringUtils.join(tokenIds.toArray(), ", ")+" ";
			}
		}
	}
	
	protected void addHitContext(String with, String group) {
		String type = groupToType(group);
		if (group.contains("_left") && query.getContextSize() == 1) {
			hitContext = " MATCH ("+typeToId(type)+":"+type+")<-["+typeToRelationship(type)+"]-(:WordToken)-[:NEXT]->(t1) "
					+ "WITH "+with+", "+typeToId(type)+".label AS "+group+" ";
//			hitContext = " MATCH (left:WordToken)-[:NEXT]->t1 "
//					+ "WITH "+with+", left "
//					+ "ORDER BY left.token_index "
//					+ "WITH "+with+", COLLECT(DISTINCT left) AS "+group+" "
//					+ "UNWIND "+group+" AS token "
//					+ " MATCH token-["+typeToRelationship(type)+"]->("+typeToId(type)+":"+type+") "
//					+ "WITH "+with+", "+typeToId(type)+".label AS "+group+" ";
		} else if (group.contains("_right") && query.getContextSize() == 1) {
			hitContext = " MATCH ("+typeToId(type)+":"+type+")<-["+typeToRelationship(type)+"]-(:WordToken)<-[:NEXT]-(t1) "
					+ "WITH "+with+", "+typeToId(type)+".label AS "+group+" ";
//			hitContext = " MATCH t"+String.valueOf(tokenIds.size())+"-[:NEXT]->(right:WordToken) "
//					+ "WITH "+with+", right "
//					+ "ORDER BY right.token_index "
//					+ "WITH "+with+", COLLECT(DISTINCT right) AS "+group+" "
//					+ "UNWIND "+group+" AS token "
//					+ " MATCH token-["+typeToRelationship(type)+"]->("+typeToId(type)+":"+type+") "
//					+ "WITH "+with+", "+typeToId(type)+".label AS "+group+" ";
		} else {
			hitContext = " MATCH (left:WordToken)-[:NEXT*1.."+String.valueOf(query.getContextSize())+"]->(t1) "
					+ " MATCH (t"+String.valueOf(tokenIds.size())+")-[:NEXT*1.."+String.valueOf(query.getContextSize())+"]->(right:WordToken) "
					+ "WITH "+with+", left, right "
					+ "ORDER BY left.token_index, right.token_index "
					+ "WITH "+with+", COLLECT(DISTINCT left) AS "+group+"_left, COLLECT(DISTINCT right) AS "+group+"_right "
					+ "UNWIND "+group+"_left AS token "
					+ " MATCH (token)-["+typeToRelationship(type)+"]->("+typeToId(type)+":"+type+") "
					+ "WITH "+with+", COLLECT("+typeToId(type)+".label) AS "+group+"_left, "+group+"_right "
					+ "UNWIND "+group+"_right AS token "
					+ " MATCH (token)-["+typeToRelationship(type)+"]->("+typeToId(type)+":"+type+") "
					+ "WITH "+with+", "+group+"_left, COLLECT("+typeToId(type)+".label) AS "+group+"_right ";
		}
	}

	protected void addDocHitsPattern() {
		System.out.println("addDocHitsPattern");
		
		List<String> filters = new ArrayList<String>();
		filters.add(" MATCH (document)-[:HAS_TOKEN]->(t1:WordToken)");
		
		JSONObject columns = query.toJSON();
		for (int c = 1; c <= query.getColumnCount(); c++) {
			if (c > 1) {
				int cp = c - 1;
				filters.add(" MATCH (t"+String.valueOf(cp)+")-[:NEXT]->(t"+String.valueOf(c)+":WordToken)");
			}
			String columnKey = String.valueOf(c);
			JSONObject column = columns.getJSONObject(columnKey);
			filters.add(getDocHitsColumn(column, columnKey));
			String where = getDocHitsColumnWhereClause(column, columnKey);
			if (where != null && where.length() > 0)
				filters.add("WHERE "+where);
		}
		filters.add("WITH document, ["+StringUtils.join(tokenIds.toArray(), ", ")+"] AS hit");
		
		docHitsPattern = StringUtils.join(filters.toArray(), " ");
	}
	
	private String getDocHitsColumn(JSONObject column, String columnKey) {
		System.out.println("getDocHitsColumn");
		if (column.has("repeat_of")) {
			if (column.has("field")) {
				if (column.getString("field").equals("PosHead"))
					return " MATCH (t"+columnKey+")-[:HAS_POS_TAG]->(:PosTag)-["+typeToRelationship(column.getString("field"))+"]->(:"+column.getString("field")+")<-["+typeToRelationship(column.getString("field"))+"]-(:PosTag)<-[:HAS_POS_TAG]-(t"+column.getString("repeat_of")+")";
				else
					return " MATCH (t"+columnKey+")-["+typeToRelationship(column.getString("field"))+"]->(:"+column.getString("field")+")<-["+typeToRelationship(column.getString("field"))+"]-(t"+column.getString("repeat_of")+")";
			} else {
				return " MATCH (t"+columnKey+")-[:HAS_TYPE]->(:WordType)<-[:HAS_TYPE]-(t"+column.getInt("repeat_of")+") "
					+ " MATCH (t"+columnKey+")-[:HAS_LEMMA]->(:Lemma)<-[:HAS_LEMMA]-(t"+column.getInt("repeat_of")+") "
					+ " MATCH (t"+columnKey+")-[:HAS_POS_TAG]->(:PosTag)<-[:HAS_POS_TAG]-(t"+column.getInt("repeat_of")+")";
			}
		} else {
			JSONArray fields = column.getJSONArray("fields");
			List<String> filters = new ArrayList<String>();
			for (int f = 0; f < fields.length(); f++) {
				JSONObject field = fields.getJSONObject(f);
				if (field.has("fields") || field.has("repeat_of"))
					filters.add(getDocHitsColumn(field, columnKey));
				else if (field.getString("field").equals("PosHead")) {
					filters.add(" MATCH (t"+columnKey+")-[:HAS_POS_TAG]->(:PosTag)-["+typeToRelationship(field.getString("field"))+"]->("+generateFieldKey(field.getString("field"), columnKey, 0)+":"+field.getString("field")+")");
				} else {
					filters.add(" MATCH (t"+columnKey+")-["+typeToRelationship(field.getString("field"))+"]->("+generateFieldKey(field.getString("field"), columnKey, 0)+":"+field.getString("field")+")");
				}
			}
			return StringUtils.join(filters.toArray(), " ");
		}
	}
	
	private String getDocHitsColumnWhereClause(JSONObject column, String columnKey) {
		System.out.println("getDocHitsColumnWhereClause");
		List<String> filters = new ArrayList<String>();
		if (!column.has("repeat_of")) {
			JSONArray fields = column.getJSONArray("fields");
			System.out.println("FIELDS: "+String.valueOf(fields.length()));
			for (int f = 0; f < fields.length(); f++) {
				JSONObject field = fields.getJSONObject(f);
				if (field.has("fields") || field.has("repeat_of")) {
					System.out.println("*** INFO: A");
					filters.add(getDocHitsColumnWhereClause(field, columnKey));
				} else if (field.has("field")) {
					System.out.println("*** INFO: B");
					String fieldType = field.getString("field");
					boolean sensitive = false;
					if (field.has("case_sensitive"))
						sensitive = field.getBoolean("case_sensitive");
					System.out.println("*** INFO: case_sensitive: "+String.valueOf(sensitive));
					if ((fieldType.equals("WordType") || fieldType.equals("Lemma")) && !sensitive) {
						System.out.println("*** INFO: Ba");
						filters.add(generateFieldKey(fieldType, columnKey, 0)+".label =~ '(?i)"+field.getString("pattern")+"'");
					} else {
						System.out.println("*** INFO: Bb");
						filters.add(generateFieldKey(fieldType, columnKey, 0)+".label = '"+field.getString("pattern")+"'");
					}
				}
			}
			
		}
		
		if (filters.size() == 0)
			return "";
		else if (filters.size() == 1)
			return filters.get(0);
		else {
			String operator = "AND";
			if (column.has("operator"))
				operator = column.getString("operator");
			return "("+StringUtils.join(filters.toArray(), " "+operator+" ")+")";
		}
	}
	
	protected void addFilterGrouping() {
//		filterGrouping = " MATCH (document:Document)-[:HAS_TOKEN]->t1";
		filterGrouping = "";
		List<String> corpusNodes = getStartNodesForType("Corpus");
		List<String> collectionNodes = getStartNodesForType("Corpus");
		if (corpusNodes.size() == 0 && collectionNodes.size() == 0) {
			if (query.getGroup().equals("Corpus_title")) {
				filterGrouping = filterGrouping + " MATCH (corpus)-[:HAS_COLLECTION]->(:Collection)-[:HAS_DOCUMENT]->(document) "
					+ "WITH corpus.title AS Corpus_title ";
				if (hasEmptyPattern)
					filterGrouping = filterGrouping + ", corpus ";
			} else if (query.getGroup().equals("Collection_title")) {
				filterGrouping = filterGrouping + " MATCH (collection:Collection)-[:HAS_DOCUMENT]->(document) "
					+ "WITH collection.title AS Collection_title ";
				if (hasEmptyPattern)
					filterGrouping = filterGrouping + ", collection ";
			} else {
				String group = query.getGroup().split("_")[0];
				String key = query.getGroup().replaceFirst(group+"_", "");
				filterGrouping = filterGrouping + " OPTIONAL MATCH (document)-[r:HAS_METADATUM]->(:Metadatum{group:'"+group+"',key:'"+key+"'}) "
					+ "WITH DISTINCT r.value AS "+query.getGroup();
				if (hasEmptyPattern)
					filterGrouping = filterGrouping + ", document ";
			}
			if (!hasEmptyPattern) {
				if (query.getView() == 16)
					filterGrouping = filterGrouping + ", document";
				else
					filterGrouping = filterGrouping + ", " + StringUtils.join(tokenIds.toArray(), ", ");
			}
		} else {
			List<String> where = new ArrayList<String>();
			filterGrouping = " MATCH ";
			if (corpusNodes.size() > 0) {
				filterGrouping = filterGrouping + "(corpus)-[:HAS_COLLECTION]->";
				where.add("corpus IN ["+StringUtils.join(corpusNodes.toArray(), ", ")+"]");
			}
			
			if (collectionNodes.size() > 0) {
				filterGrouping = filterGrouping + "(collection:Collection)-[:HAS_DOCUMENT]->";
				where.add("collection IN ["+StringUtils.join(collectionNodes.toArray(), ", ")+"]");
			}

			filterGrouping = filterGrouping + "(document) ";
			
			if (where.size() > 0)
				filterGrouping = filterGrouping + "WHERE "+StringUtils.join(where.toArray(), " AND ")+" ";
			
			if (query.getGroup().equals("Corpus_title")) {
				filterGrouping = filterGrouping + "WITH corpus.title AS Corpus_title ";
				if (hasEmptyPattern)
					filterGrouping = filterGrouping + ", corpus ";
			} else if (query.getGroup().equals("Collection_title")) {
				filterGrouping = filterGrouping + "WITH collection.title AS Collection_title ";
				if (hasEmptyPattern)
					filterGrouping = filterGrouping + ", collection ";
			} else {
				String group = query.getGroup().split("_")[0];
				String key = query.getGroup().replaceFirst(group+"_", "");
				filterGrouping = filterGrouping + " OPTIONAL MATCH (document)-[r:HAS_METADATUM]->(:Metadatum{group:'"+group+"',key:'"+key+"'}) "
					+ "WITH DISTINCT r.value AS "+query.getGroup();
				if (hasEmptyPattern)
					filterGrouping = filterGrouping + ", document ";
			}
			if (!hasEmptyPattern) {
				if (query.getView() == 16)
					filterGrouping = filterGrouping + ", document";
				else
					filterGrouping = filterGrouping + ", " + StringUtils.join(tokenIds.toArray(), ", ");
			}
		}
	}

}
