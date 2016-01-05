package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class DocumentVocabularyGrowthQueryTemplate extends CypherQuery {
	
	public DocumentVocabularyGrowthQueryTemplate() {
		
	}

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setResultHeader("content");
		addHitContent(null);
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
		
		cypher.add("RETURN DISTINCT document.xmlid, t1.token_index, hit_text, hit_lemma, hit_pos, hit_phonetic");

		cypher.add(tokenPattern);

		return StringUtils.join(cypher.toArray(), " ");
	}

}
