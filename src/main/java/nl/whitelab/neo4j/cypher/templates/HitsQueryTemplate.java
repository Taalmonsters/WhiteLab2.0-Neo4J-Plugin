package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class HitsQueryTemplate extends CypherQuery {
	
	public HitsQueryTemplate() {
		
	}

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setResultHeader("hits");
		if (!query.isCountQuery()) {
			addHitContent(null);
			addHitContext("document, hit_text, hit_lemma, hit_pos, hit_phonetic, first_index, last_index, begin_time, end_time, "+StringUtils.join(tokenIds.toArray(), ", "), "text");
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
//			cypher.add("WITH DISTINCT document ");
//			
//			List<String> contentNodes = getStartNodesForTypes(new String[]{"WordType", "Lemma", "PosTag", "PosHead", "Phonetic"});
//			for (String node : contentNodes) {
//				cypher.add(", "+node);
//			}
		}
		
		cypher.add(tokenPattern);
		
		cypher.add("WITH DISTINCT document, "+StringUtils.join(tokenIds.toArray(), ", ")+" "
				+ "SKIP "+String.valueOf(query.getSkip())+" ");
		if (query.getLimit() > 0)
			cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
		
		if (query.isCountQuery())
			cypher.add("RETURN COUNT(t1) AS hit_count;");
		else {
			cypher.add(hitContent);
			cypher.add(hitContext);
			cypher.add("MATCH (document)<-[:HAS_DOCUMENT]-(collection:Collection) "
					+ "MATCH (collection)<-[:HAS_COLLECTION]-(corpus:Corpus)");
			cypher.add("RETURN corpus.title AS corpus, collection.title AS collection, document.xmlid AS docpid, first_index, last_index, begin_time, end_time, hit_text, hit_lemma, hit_pos, trim(hit_phonetic) AS hit_phonetic, text_left, text_right;");
		}
		
		return StringUtils.join(cypher.toArray(), " ");
	}
}
