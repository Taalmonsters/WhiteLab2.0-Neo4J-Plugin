package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class GroupedDocsQueryTemplate extends CypherQuery {
	
	public GroupedDocsQueryTemplate() {
		
	}

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setResultHeader("grouped_docs");
		addFilterGrouping();
	}

	@Override
	public String toQueryString() {
		List<String> cypher = new ArrayList<String>();
		
		if (startNodes.size() > 0) {
			List<String> start = new ArrayList<String>();
			for (String nodeId : startNodes.keySet()) {
				start.add(nodeId+"="+startNodes.get(nodeId));
			}
			cypher.add("START "+StringUtils.join(start.toArray(), ", "));
		}

		if (metadataFilter != null && metadataFilter.length() > 0) {
			cypher.add(metadataFilter);
//			cypher.add("WITH DISTINCT document ");
//			
//			List<String> contentNodes = getStartNodesForTypes(new String[]{"WordType", "Lemma", "PosTag", "PosHead", "Phonetic"});
//			for (String node : contentNodes) {
//				cypher.add(", "+node);
//			}
		}

		cypher.add(tokenPattern);
		
		cypher.add(filterGrouping);
		
		if (query.isCountQuery()) {
			cypher.add("WITH DISTINCT "+query.getGroup()+" "
					+ "SKIP "+String.valueOf(query.getSkip())+" ");
			if (query.getLimit() > 0)
				cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			cypher.add("RETURN COUNT(*) AS group_count;");
		} else {
			cypher.add("RETURN DISTINCT "+query.getGroup()+", COUNT(DISTINCT document) AS document_count "
					+ "ORDER BY "+getQuerySort()+" "+getQueryOrder()+" "
					+ "SKIP "+String.valueOf(query.getSkip())+" ");
			if (query.getLimit() > 0)
				cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
		}
		
		return StringUtils.join(cypher.toArray(), " ");
	}
	
	private String getQuerySort() {
		String sort = query.getSort();
		if (sort == null) {
			return "document_count";
		}
		return sort;
	}
	
	private String getQueryOrder() {
		String order = query.getOrder();
		if (order == null) {
			return "desc";
		}
		return order;
	}
}
