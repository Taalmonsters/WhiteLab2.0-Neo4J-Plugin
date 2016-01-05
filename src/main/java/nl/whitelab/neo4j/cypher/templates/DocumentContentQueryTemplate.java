package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class DocumentContentQueryTemplate extends CypherQuery {

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setResultHeader("content");
	}

	@Override
	public String toQueryString() {
		List<String> cypher = new ArrayList<String>();
		
		if (hasEmptyPattern) {
			cypher.add("START document=node:Document('xmlid:"+query.getDocpid()+"') "
					+ "MATCH (s:Sentence)-[:OCCURS_IN]->(document) "
					+ "WITH document, COUNT(DISTINCT s) AS total_sentence_count "
					+ "MATCH (s:Sentence)-[:OCCURS_IN]->(document) "
					+ "WITH DISTINCT document, total_sentence_count, s "
					+ "ORDER BY s.start_index "
					+ "WITH document, total_sentence_count, s "
					+ "SKIP "+String.valueOf(query.getSkip())+" ");
			
			if (query.getLimit() > 0)
				cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			
			cypher.add("WITH document, MIN(s.start_index) AS min_index, MAX(s.end_index) AS max_index, total_sentence_count "
					+ "MATCH (document)-[:HAS_TOKEN]->(t:WordToken) "
					+ "WHERE t.token_index >= min_index AND t.token_index <= max_index "
					+ "WITH document.mp3 AS audio_file, total_sentence_count, COLLECT(t) AS tokens "
					+ "UNWIND tokens AS t "
					+ "MATCH (t)-[:HAS_TYPE]->(w:WordType) "
					+ "MATCH (t)-[:HAS_LEMMA]->(l:Lemma) "
					+ "MATCH (t)-[:HAS_POS_TAG]->(p:PosTag) "
					+ "MATCH (t)-[:HAS_PHONETIC]->(ph:Phonetic) "
					+ "WITH audio_file, total_sentence_count, t, w.label AS word_type, l.label AS lemma, p.label AS pos_tag, ph.label AS phonetic "
					+ "OPTIONAL MATCH (t)<-[r:STARTS_AT]-(s:Sentence) "
					+ "WITH audio_file, total_sentence_count, t, word_type, lemma, pos_tag, phonetic, CASE WHEN s IS NOT NULL THEN 'true' ELSE 'false' END AS sentence_start, CASE WHEN r.actor_code IS NOT NULL THEN r.actor_code ELSE '' END AS speaker "
					+ "OPTIONAL MATCH (t)<-[:STARTS_AT]-(p:ParagraphStart) "
					+ "WITH audio_file, total_sentence_count, t, word_type, lemma, pos_tag, phonetic, sentence_start, speaker, CASE WHEN p IS NOT NULL THEN 'true' ELSE 'false' END AS paragraph_start "
					+ "ORDER BY t.token_index "
					+ "RETURN audio_file, total_sentence_count, COLLECT({ token_index: t.token_index, xmlid: t.xmlid, begin_time: t.begin_time, end_time: t.end_time, word_type: word_type, lemma: lemma, pos_tag: pos_tag, phonetic: phonetic, sentence_speaker: speaker, sentence_start: sentence_start, paragraph_start: paragraph_start }) AS content;");
		} else {
			if (startNodes.size() > 0) {
				List<String> start = new ArrayList<String>();
				for (String nodeId : startNodes.keySet()) {
					start.add(nodeId+"="+startNodes.get(nodeId));
				}
				cypher.add("START "+StringUtils.join(start.toArray(), ", "));
			}
			
			cypher.add(tokenPattern);
			
			cypher.add("WITH DISTINCT document, ["+StringUtils.join(tokenIds.toArray(), ".token_index, ")+".token_index] AS hit "
					+ "WITH document, COLLECT(hit) AS hits "
					+ "WITH document, REDUCE(output = [], r IN hits | output + r) AS hits "
					+ "MATCH (s:Sentence)-[:OCCURS_IN]->(document) "
					+ "WITH document, hits, COUNT(DISTINCT s) AS total_sentence_count "
					+ "MATCH (s:Sentence)-[:OCCURS_IN]->(document) "
					+ "WITH DISTINCT document, hits, total_sentence_count, s "
					+ "ORDER BY s.start_index "
					+ "WITH document, hits, total_sentence_count, s "
					+ "SKIP "+String.valueOf(query.getSkip())+" ");
			
			if (query.getLimit() > 0)
				cypher.add("LIMIT "+String.valueOf(query.getLimit())+" ");
			
			cypher.add("WITH document, hits, MIN(s.start_index) AS min_index, MAX(s.end_index) AS max_index, total_sentence_count "
					+ "MATCH (document)-[:HAS_TOKEN]->(t:WordToken) "
					+ "WHERE t.token_index >= min_index AND t.token_index <= max_index "
					+ "WITH document.mp3 AS audio_file, total_sentence_count, COLLECT(t) AS tokens, hits "
					+ "UNWIND tokens AS t "
					+ "MATCH (t)-[:HAS_TYPE]->(w:WordType) "
					+ "MATCH (t)-[:HAS_LEMMA]->(l:Lemma) "
					+ "MATCH (t)-[:HAS_POS_TAG]->(p:PosTag) "
					+ "MATCH (t)-[:HAS_PHONETIC]->(ph:Phonetic) "
					+ "WITH audio_file, total_sentence_count, t, w.label AS word_type, l.label AS lemma, p.label AS pos_tag, ph.label AS phonetic, "
					+ "CASE WHEN t.token_index IN hits THEN true ELSE false END AS hit "
					+ "OPTIONAL MATCH (t)<-[r:STARTS_AT]-(s:Sentence) "
					+ "WITH audio_file, total_sentence_count, t, word_type, lemma, pos_tag, phonetic, hit, CASE WHEN s IS NOT NULL THEN 'true' ELSE 'false' END AS sentence_start, CASE WHEN r.actor_code IS NOT NULL THEN r.actor_code ELSE '' END AS speaker "
					+ "OPTIONAL MATCH (t)<-[:STARTS_AT]-(p:ParagraphStart) "
					+ "WITH audio_file, total_sentence_count, t, word_type, lemma, pos_tag, phonetic, hit, sentence_start, speaker, CASE WHEN p IS NOT NULL THEN 'true' ELSE 'false' END AS paragraph_start "
					+ "ORDER BY t.token_index "
					+ "RETURN audio_file, total_sentence_count, COLLECT({ token_index: t.token_index, xmlid: t.xmlid, hit: hit, begin_time: t.begin_time, end_time: t.end_time, word_type: word_type, lemma: lemma, pos_tag: pos_tag, phonetic: phonetic, sentence_speaker: speaker, sentence_start: sentence_start, paragraph_start: paragraph_start }) AS content;");
		}
		
		return StringUtils.join(cypher.toArray(), " ");
	}

}
