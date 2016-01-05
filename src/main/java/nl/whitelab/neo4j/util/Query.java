package nl.whitelab.neo4j.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class Query {
	private String id = null;
	private String cql = null;
	private String within = "document";
	private String filter = null;
	private int view = 1;
	private String sort = null;
	private String order = null;
	private String group = null;
	private String selected_group = null;
	private int skip = 0;
	private int limit = 50;
	private int contextSize = 5;
	private boolean count = false;
	private String docpid = null;
	private JSONObject json;
	private String cypher;
	private Result result = null;
	private long start;
	private long end;
	private Map<String,Map<String,List<Integer>>> columnFieldIds = new HashMap<String,Map<String,List<Integer>>>();
	private String resultHeader = "results";
	
	public Query(String id, String cql, boolean count) {
		this.id = id;
		if (this.id == null)
			this.id = UUID.randomUUID().toString();
		this.cql = cql;
		this.count = count;
	}
	
	public Query(String id, String cql, String docpid) {
		this.id = id;
		if (this.id == null)
			this.id = UUID.randomUUID().toString();
		this.cql = cql;
		this.docpid = docpid;
	}
	
	public Query(String id, String cql, String within, String filter, int view, boolean count) {
		this.id = id;
		if (this.id == null)
			this.id = UUID.randomUUID().toString();
		this.cql = cql;
		this.within = within;
		this.filter = filter;
		this.view = view;
		this.count = count;
	}
	
	public Query(String id, String cql, String within, String filter, int view, int skip, int limit, int contextSize, boolean count) {
		this.id = id;
		if (this.id == null)
			this.id = UUID.randomUUID().toString();
		this.cql = cql;
		this.within = within;
		this.filter = filter;
		this.view = view;
		this.skip = skip;
		this.limit = limit;
		this.contextSize = contextSize;
		this.count = count;
	}
	
	public Query(String id, JSONObject json, boolean count) {
		this.id = id;
		if (this.id == null)
			this.id = UUID.randomUUID().toString();
		this.count = count;
		this.json = json;
	}
	
	public Query(String id, JSONObject json, String docpid) {
		this.id = id;
		if (this.id == null)
			this.id = UUID.randomUUID().toString();
		this.docpid = docpid;
	}
	
	public Query(String id, JSONObject json, String within, String filter, int view, boolean count) {
		this.id = id;
		if (this.id == null)
			this.id = UUID.randomUUID().toString();
		this.within = within;
		this.filter = filter;
		this.view = view;
		this.count = count;
		this.json = json;
	}
	
	public Query(String id, JSONObject json, String within, String filter, int view, int skip, int limit, int contextSize, boolean count) {
		this.id = id;
		if (this.id == null)
			this.id = UUID.randomUUID().toString();
		this.within = within;
		this.filter = filter;
		this.view = view;
		this.skip = skip;
		this.limit = limit;
		this.contextSize = contextSize;
		this.count = count;
		this.json = json;
	}
	
//	public void parse() {
//		if (this.json == null && this.cql != null)
//			this.json = cqlToJson();
//		System.out.println("*** INFO: QUERY JSON:");
//		System.out.println(this.toJSON().toString());
//		if (this.cypher == null && this.json != null)
//			this.cypher = jsonToCypher();
//	}
	
	public String getId() {
		return id;
	}
	
	public void setWithin(String within) {
		this.within = within;
	}
	
	public String getWithin() {
		return within;
	}
	
	public void setResultHeader(String r) {
		this.resultHeader = r;
	}
	
	public String getResultHeader() {
		return resultHeader;
	}
	
	public void setFilter(String filter) {
		this.filter = filter;
	}
	
	public String getFilter() {
		return filter;
	}
	
	public void setView(int view) {
		this.view = view;
	}
	
	public int getView() {
		return view;
	}
	
	public void setSort(String sort) {
		this.sort = sort;
	}
	
	public String getSort() {
		return sort;
	}
	
	public void setOrder(String order) {
		this.order = order;
	}
	
	public String getOrder() {
		return order;
	}
	
	public void setGroup(String group) {
		this.group = group;
	}
	
	public String getGroup() {
		return group;
	}
	
	public void setSelectedGroup(String selected_group) {
		this.selected_group = selected_group;
	}
	
	public String getSelectedGroup() {
		return selected_group;
	}
	
	public void setSkip(int skip) {
		this.skip = skip;
	}
	
	public int getSkip() {
		return skip;
	}
	
	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	public int getLimit() {
		return limit;
	}
	
	public void setDocpid(String docpid) {
		this.docpid = docpid;
	}
	
	public String getDocpid() {
		return docpid;
	}
	
	public boolean isDocumentQuery() {
		return docpid != null;
	}
	
	public void setContextSize(int contextSize) {
		this.contextSize = contextSize;
	}
	
	public int getContextSize() {
		return contextSize;
	}
	
	public int getColumnCount() {
		return json.keySet().size();
	}
	
	public Long getDuration() {
		return end - start;
	}
	
	public Map<String,Map<String,List<Integer>>> getColumnFieldIds() {
		return columnFieldIds;
	}
	
	public void setColumnFieldIds(Map<String,Map<String,List<Integer>>> ids) {
		columnFieldIds = ids;
	}
	
	public void addColumnFieldId(String type, String columnKey, Integer fieldCount) {
		if (!columnFieldIds.containsKey(columnKey))
			columnFieldIds.put(columnKey, new HashMap<String,List<Integer>>());
		if (!columnFieldIds.get(columnKey).containsKey(type))
			columnFieldIds.get(columnKey).put(type, new ArrayList<Integer>());
		if (!columnFieldIds.get(columnKey).get(type).contains(fieldCount))
			columnFieldIds.get(columnKey).get(type).add(fieldCount);
	}
	
//	public void addFieldKey(String fieldKey) {
//		if (!fieldKeys.contains(fieldKey))
//			fieldKeys.add(fieldKey);
//	}
//	
//	public List<String> getFieldKeys() {
//		return fieldKeys;
//	}
	
	public boolean isCountQuery() {
		return count;
	}
	
	public String toCQL() {
		return cql;
	}
	
	public JSONObject toJSON() {
		return json;
	}
	
	public void setJSON(JSONObject json) {
		this.json = json;
	}
	
	public String toCypher() {
		return cypher;
	}
	
	public void setCypher(String cypher) {
		this.cypher = cypher;
	}
	
	public void setStart(Long start) {
		this.start = start;
	}
	
	public void setEnd(Long end) {
		this.end = end;
	}
	
	public Boolean hasEmptyPattern() {
		if (this.cql != null && (this.cql.equals("[]") || this.cql.equals("[word=\".*\"]") || this.cql.length() == 0))
			return true;
		else if (this.json != null && this.json.length() == 1 && this.json.getJSONObject("1").has("fields")) {
			JSONObject firstField = (JSONObject) this.json.getJSONObject("1").getJSONArray("fields").get(0);
			return firstField.has("pattern") && firstField.getString("pattern").equals(".*");
		} else if (this.json == null && this.cql == null) {
			return true;
		}
		return false;
	}
	
	public Boolean hasEmptyFilter() {
		if (this.filter == null || this.filter.length() == 0)
			return true;
		return false;
	}
	
	public Result execute(GraphDatabaseService database) {
		if (result == null) {
			start = System.currentTimeMillis();
			try ( Transaction ignored = database.beginTx(); Result queryResult = database.execute(cypher) ) {
				end = System.currentTimeMillis();
				result = queryResult;
			}
		}
		return result;
	}
	
//	private JSONObject cqlToJson() {
//		if (cql != null) {
//			CQLParser parser = new CQLParser();
//			return parser.CQLtoJSON(cql);
//		}
//		return null;
//	}
	
//	private String jsonToCypher() {
//		if (json != null) {
//			CopyOfCypherConstructor constructor = new CopyOfCypherConstructor(this);
//			return constructor.getCypher();
//		}
//		return null;
//	}

}
