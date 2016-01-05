package nl.whitelab.neo4j.cypher.templates;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import nl.whitelab.neo4j.cypher.CypherQuery;
import nl.whitelab.neo4j.util.Query;

public class TotalSizeVsDocumentSizeQueryTemplate extends CypherQuery {
	
	public TotalSizeVsDocumentSizeQueryTemplate() {
		
	}

	@Override
	public void applyTemplate(Query q) {
		beforeTemplate(q);
		query.setResultHeader("results");
	}

	@Override
	public String toQueryString() {
		System.out.println("TotalSizeVsDocumentSizeQueryTemplate.toQueryString");
		List<String> cypher = new ArrayList<String>();
		
		if (metadataFilter != null && metadataFilter.length() > 0) {
			if (startNodes.size() > 0) {
				List<String> start = new ArrayList<String>();
				for (String nodeId : startNodes.keySet()) {
					start.add(nodeId+"="+startNodes.get(nodeId));
				}
				cypher.add("START "+StringUtils.join(start.toArray(), ", "));
			}
			cypher.add(metadataFilter);
			if (query.getGroup().startsWith("Corpus")) {
				cypher.add("WITH DISTINCT corpus, document "
						+ "WITH corpus.title AS title, SUM(document.token_count) AS total_size, COUNT(document) AS document_count "
						+ "RETURN title, total_size, total_size / document_count AS avg_document_size, document_count;");
			} else if (query.getGroup().startsWith("Collection")) {
				cypher.add("WITH DISTINCT collection, document "
						+ "WITH collection.title AS title, SUM(document.token_count) AS total_size, COUNT(document) AS document_count "
						+ "RETURN title, total_size, total_size / document_count AS avg_document_size, document_count;");
			} else {
				String group = query.getGroup().split("_")[0];
				String key = query.getGroup().replace(group+"_", "");
				cypher.add("WITH DISTINCT document "
						+ "MATCH (metadatum:Metadatum{group:'"+group+"',key:'"+key+"'}) "
						+ "MATCH (metadatum)-[r:HAS_METADATUM]-document ");
				
				if (query.getFilter().contains(group+"_"+key)) {
					List<String> filterValues = getValuesForFilter(group, key, query.getFilter());
					cypher.add("WHERE TOSTRING(r.value) IN ['"+StringUtils.join(filterValues.toArray(),"','")+"'] ");
				}
				
				cypher.add("WITH DISTINCT r.value AS title, document "
						+ "WITH title, COUNT(document) AS document_count, SUM(document.token_count) AS total_size "
						+ "RETURN title, total_size, total_size / document_count AS avg_document_size, document_count "
						+ "UNION ");
				
				if (startNodes.size() > 0) {
					List<String> start = new ArrayList<String>();
					for (String nodeId : startNodes.keySet()) {
						start.add(nodeId+"="+startNodes.get(nodeId));
					}
					cypher.add("START "+StringUtils.join(start.toArray(), ", "));
				}
				cypher.add(metadataFilter);
				
				cypher.add("WITH DISTINCT document "
						+ "MATCH (metadatum:Metadatum{group:'"+group+"',key:'"+key+"'}) "
						+ "MATCH (document) ");
				
				if (query.getFilter().contains(group+"_"+key)) {
					List<String> filterValues = getValuesForFilter(group, key, query.getFilter());
					for (int f = 0; f < filterValues.size(); f++) {
						String filterValue = filterValues.get(f);
						if (f == 0)
							cypher.add("WHERE ");
						else
							cypher.add("AND ");
						cypher.add("NOT (document)-[:HAS_METADATUM{value:'"+filterValue+"'}]->(metadatum) ");
					}
				} else {
					cypher.add("WHERE NOT (document)-[:HAS_METADATUM]->(metadatum) ");
				}
				
				cypher.add("WITH 'Unknown' AS title, SUM(document.token_count) AS total_size, COUNT(DISTINCT document) AS document_count "
						+ "RETURN title, total_size, total_size / document_count AS avg_document_size, document_count;");
			}
		} else {
			if (query.getGroup().startsWith("Corpus")) {
				cypher.add("MATCH (corpus:Corpus) "
						+ "RETURN corpus.title AS title, corpus.token_count AS total_size, corpus.token_count / corpus.document_count AS avg_document_size, corpus.document_count AS document_count;");
			} else if (query.getGroup().startsWith("Collection")) {
				cypher.add("MATCH (collection:Collection) "
						+ "RETURN collection.title AS title, collection.token_count AS total_size, collection.token_count / collection.document_count AS avg_document_size, collection.document_count AS document_count;");
			} else {
				String group = query.getGroup().split("_")[0];
				String key = query.getGroup().replace(group+"_", "");
				cypher.add("START metadatum=node:Metadatum('group:"+group+" AND key:"+key+"') "
						+ "MATCH (metadatum)-[r:HAS_METADATUM]-(document:Document) "
						+ "WITH DISTINCT r.value AS title, document "
						+ "WITH title, COUNT(document) AS document_count, SUM(document.token_count) AS total_size "
						+ "RETURN title, total_size, total_size / document_count AS avg_document_size, document_count "
						+ "UNION "
						+ "START metadatum=node:Metadatum('group:"+group+" AND key:"+key+"') "
						+ "MATCH (document:Document) "
						+ "WHERE NOT (document)-[:HAS_METADATUM]->(metadatum) "
						+ "WITH DISTINCT 'Unknown' AS title, document "
						+ "WITH title, SUM(document.token_count) AS total_size, COUNT(document) AS document_count "
						+ "RETURN title, total_size, total_size / document_count AS avg_document_size, document_count;");
			}
		}
		
		return StringUtils.join(cypher.toArray(), " ");
	}

	private List<String> getValuesForFilter(String group, String key, String filter) {
		List<String> filterValues = new ArrayList<String>();
		String[] filters = filter.substring(1, filter.length()-1).split("\\)AND\\(");
		for (int f = 0; f < filters.length; f++) {
			Map<String, String> metadatum = parseMetadataFieldString(filters[f]);
			if (metadatum.get("group").equals(group) && metadatum.get("key").equals(key) && metadatum.get("operator").equals("=")) {
				filterValues.add(metadatum.get("value"));
			}
		}
		return filterValues;
	}
}
