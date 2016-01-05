package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class DocumentCountFromNodesQueryTemplate extends CypherQuery {
	
	public DocumentCountFromNodesQueryTemplate() {
		
	}

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setResultHeader("grouped_docs");
	}

	@Override
	public String toQueryString() {
		System.out.println("DocumentCountFromNodesQueryTemplate.toQueryString");
		List<String> cypher = new ArrayList<String>();
		String group = query.getGroup().split("_")[0];
		String key = query.getGroup().replaceFirst(group+"_", "");
		
		
		if (hasEmptyFilter)
			cypher.add("MATCH (n:"+group+") RETURN n."+key+" AS "+query.getGroup()+", n.document_count AS document_count;");
		else {
			if (startNodes.size() > 0) {
				List<String> start = new ArrayList<String>();
				for (String nodeId : startNodes.keySet()) {
					start.add(nodeId+"="+startNodes.get(nodeId));
				}
				cypher.add("START "+StringUtils.join(start.toArray(), ", "));
			}
			cypher.add(metadataFilter);
			
//			if (group.equals("Corpus") || group.equals("Collection"))
				cypher.add(" RETURN DISTINCT "+group.toLowerCase()+"."+key+" AS "+query.getGroup()+", COUNT(DISTINCT document) AS document_count;");
//			else
//				cypher.add("MATCH (:Metadatum{group:'"+group+"',key:'"+key+"'})<-[r:HAS_METADATUM]-document "
//						+ "WITH DISTINCT r.value AS "+query.getGroup()+", document "
//						+ "RETURN "+query.getGroup()+", SUM(document.token_count) AS hit_count;");
		}

		return StringUtils.join(cypher.toArray(), " ");
	}

}
