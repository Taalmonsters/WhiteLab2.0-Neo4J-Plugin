package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class DocsQueryTemplate extends CypherQuery {

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setResultHeader("docs");
		if (!query.isCountQuery()) {
			addDocHitsPattern();
		}
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
		}

		cypher.add(tokenPattern);
		
		cypher.add("WITH DISTINCT document "
				+ "SKIP "+String.valueOf(query.getSkip())+" ");
		if (query.getLimit() > 0)
			cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
		
		if (query.isCountQuery())
			cypher.add("RETURN COUNT(document) AS document_count;");
		else {
			cypher.add(docHitsPattern);
			cypher.add("WITH document, COUNT(DISTINCT hit) AS hit_count "
					+ "MATCH (corpus:Corpus)-[:HAS_COLLECTION]->(collection:Collection) "
					+ "MATCH (collection)-[:HAS_DOCUMENT]->(document) "
					+ "RETURN corpus.title AS corpus, collection.title AS collection, document.xmlid AS docpid, hit_count;");
		}
		
		return StringUtils.join(cypher.toArray(), " ");
	}

}
