package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class GroupedHitsQueryTemplate extends CypherQuery {
	
	public GroupedHitsQueryTemplate() {
		
	}

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setContextSize(1);
		query.setResultHeader("grouped_hits");
		if (query.getGroup().startsWith("hit_")) {
			addHitContent(new String[]{query.getGroup()});
		} else if (query.getGroup().contains("left") || query.getGroup().contains("right")) {
			addHitContext("document, "+StringUtils.join(tokenIds.toArray(), ", "), query.getGroup());
		}
		addFilterGrouping();
	}

	@Override
	public String toQueryString() {
		System.out.println("GroupedHitsQueryTemplate.toQueryString");
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
		
		if (query.getGroup().startsWith("hit_")) {
			System.out.println("group: "+query.getGroup());
			cypher.add(hitContent);
		} else if (query.getGroup().contains("left") || query.getGroup().contains("right"))
			cypher.add(hitContext);
		else
			cypher.add(filterGrouping);
		
		if (query.isCountQuery()) {
			cypher.add("WITH DISTINCT "+query.getGroup()+" "
					+ "SKIP "+String.valueOf(query.getSkip())+" ");
			if (query.getLimit() > 0)
				cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			cypher.add("RETURN COUNT(*) AS group_count;");
		} else if (hasEmptyPattern) {
			if (query.getGroup().equals("Corpus_title")) {
				cypher.add("RETURN DISTINCT "+query.getGroup()+", corpus.token_count AS hit_count "
					+ "ORDER BY "+getQuerySort()+" "+getQueryOrder()+" "
					+ "SKIP "+String.valueOf(query.getSkip())+" ");
				if (query.getLimit() > 0)
					cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			} else if (query.getGroup().equals("Collection_title")) {
				cypher.add("RETURN DISTINCT "+query.getGroup()+", collection.token_count AS hit_count "
						+ "ORDER BY "+getQuerySort()+" "+getQueryOrder()+" "
						+ "SKIP "+String.valueOf(query.getSkip())+" ");
				if (query.getLimit() > 0)
					cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			} else if (query.getGroup().startsWith("hit_") || query.getGroup().contains("left") || query.getGroup().contains("right")) {
				cypher.add("RETURN DISTINCT "+query.getGroup()+", COUNT(DISTINCT t1) AS hit_count "
					+ "ORDER BY "+getQuerySort()+" "+getQueryOrder()+" "
					+ "SKIP "+String.valueOf(query.getSkip())+" ");
				if (query.getLimit() > 0)
					cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			} else {
				cypher.add("RETURN DISTINCT "+query.getGroup()+", SUM(document.token_count) AS hit_count "
					+ "ORDER BY "+getQuerySort()+" "+getQueryOrder()+" "
					+ "SKIP "+String.valueOf(query.getSkip())+" ");
				if (query.getLimit() > 0)
					cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			}
			
		} else {
			cypher.add("RETURN DISTINCT "+query.getGroup()+", COUNT(DISTINCT t1) AS hit_count "
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
