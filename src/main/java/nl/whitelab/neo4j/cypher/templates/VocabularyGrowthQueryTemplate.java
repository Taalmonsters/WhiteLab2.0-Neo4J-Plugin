package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class VocabularyGrowthQueryTemplate extends CypherQuery {
	
	public VocabularyGrowthQueryTemplate() {
		
	}

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setResultHeader("content");
		addHitContent(new String[]{"word_type", "lemma"});
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
		
		if (metadataFilter != null && metadataFilter.length() > 0)
			cypher.add(metadataFilter);

		cypher.add(tokenPattern);
		
		cypher.add("WITH DISTINCT document, "+StringUtils.join(tokenIds.toArray(), ", "));
		
		cypher.add(hitContent);
		
		cypher.add("RETURN DISTINCT document.xmlid AS docpid, t1.token_index AS token_index, word_type, lemma "
				+ "ORDER BY docpid,token_index ASC "
				+ "SKIP "+String.valueOf(query.getSkip())+" ");
		if (query.getLimit() > 0)
			cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
		
		return StringUtils.join(cypher.toArray(), " ");
	}
}
