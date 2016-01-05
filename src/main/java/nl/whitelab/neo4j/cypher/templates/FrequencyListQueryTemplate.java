package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class FrequencyListQueryTemplate extends CypherQuery {
	
	public FrequencyListQueryTemplate() {
		
	}

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setResultHeader("grouped_hits");
		if (!hasEmptyFilter) {
			addHitContent(new String[]{query.getGroup()});
		}
	}

	@Override
	public String toQueryString() {
		System.out.println("FrequencyListQueryTemplate.toQueryString");
		List<String> cypher = new ArrayList<String>();
		String rest = query.getGroup().split("_")[0];
		String group = query.getGroup().replaceFirst(rest+"_", "");
		String type = groupToType(group);
		
		if (hasEmptyFilter) {
			if (query.isCountQuery()) {
				cypher.add("MATCH (n:"+type+") RETURN COUNT(DISTINCT n) AS group_count;");
			} else {
				cypher.add("MATCH (n:"+type+") RETURN n.label AS "+query.getGroup()+", n.token_count AS hit_count ");
				cypher.add("ORDER BY "+getQuerySort()+" "+getQueryOrder()+" "
						+ "SKIP "+String.valueOf(query.getSkip())+" ");
				if (query.getLimit() > 0)
					cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			}
		} else {
			if (startNodes.size() > 0) {
				List<String> start = new ArrayList<String>();
				for (String nodeId : startNodes.keySet()) {
					start.add(nodeId+"="+startNodes.get(nodeId));
				}
				cypher.add("START "+StringUtils.join(start.toArray(), ", "));
			}
			cypher.add(metadataFilter);
			cypher.add(tokenPattern);
			cypher.add(hitContent);
			
			if (query.isCountQuery()) {
				cypher.add("RETURN COUNT(DISTINCT "+query.getGroup()+") AS group_count;");
			} else {
				cypher.add("RETURN "+query.getGroup()+", COUNT(DISTINCT t1) AS hit_count ");
				cypher.add("ORDER BY "+getQuerySort()+" "+getQueryOrder()+" "
						+ "SKIP "+String.valueOf(query.getSkip())+" ");
				if (query.getLimit() > 0)
					cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			}
		}
		
		return StringUtils.join(cypher.toArray(), " ");
	}
	
	private String getQuerySort() {
		String sort = query.getSort();
		if (sort == null) {
			return "hit_count";
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
