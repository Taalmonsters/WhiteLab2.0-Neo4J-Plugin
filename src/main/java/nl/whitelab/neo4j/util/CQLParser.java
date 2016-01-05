package nl.whitelab.neo4j.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

public class CQLParser {
	private boolean inside_dq = false;
	private boolean inside_sq = false;
	private int level = 0;
	private int max = 0;
	private boolean doLogging = false;
	
	public CQLParser() {}
	
	private void log(String msg) {
		if (doLogging)
			System.out.println(msg);
	}

	public JSONObject CQLtoJSON(String cqlString) {
		log("CQLtoJSON("+cqlString+")");
		cqlString = this.normalizeSpaces(cqlString);
		log("normalizeSpaces = "+cqlString);
		List<String> columns = this.splitColumns(cqlString);
		
		Integer in_brackets = 0;
		Map<Integer,List<Object>> sets = new HashMap<Integer,List<Object>>();
		List<Map<String,Object>> keep = new ArrayList<Map<String,Object>>();
		
		for (int c = 0; c < columns.size(); c++) {
			String columnString = columns.get(c);
			log("columnString = "+columnString);
			if (columnString.equals("(")) {
				log("column is opening bracket");
				in_brackets++;
				if (!sets.containsKey(in_brackets))
					sets.put(in_brackets, new ArrayList<Object>());
			} else if (columnString.equals(")")) {
				log("column is closing bracket");
				if (sets.containsKey(in_brackets) && sets.get(in_brackets).size() > 0) {
					List<Object> fields = (List<Object>) sets.get(in_brackets);
					Map<String,Object> column = this.processConnectors(fields);
					column = this.processQuantifiers(column);
					sets.remove(in_brackets);
					in_brackets--;
					if (in_brackets > 0)
						sets.get(in_brackets).add(column);
					else
						keep.add(column);
				} else
					in_brackets--;
			} else if (in_brackets > 0) {
				log("in_brackets larger than 0");
				sets.get(in_brackets).add(columnString);
			} else {
				log("normal column");
				Map<String,Object> column = this.processConnectors(columnString);
				column = this.processQuantifiers(column);
				keep.add(column);
			}
		}
		
		JSONObject finalColumns = new JSONObject();
		int cc = 0;
		for (int k = 0; k < keep.size(); k++) {
			Map<String,Object> column = keep.get(k);
			Integer from = (Integer) column.remove("from");
			Integer to = (Integer) column.remove("to");
			log("FROM: "+String.valueOf(from)+", TO: "+String.valueOf(to));
			if (to > 0) {
				if (from > to) {
					cc++;
					finalColumns.put(String.valueOf(cc), this.processColumnFields(column, false));
				} else {
					Boolean optional = false;
					for (int i = 1; i <= to; i++) {
						log("I: "+String.valueOf(i));
						if (i > from)
							optional = true;
						cc++;
						finalColumns.put(String.valueOf(cc), this.processColumnFields(column, optional));
					}
				}
			}
		}
		
		return finalColumns;
	}

	private String normalizeSpaces(String cqlString) {
		cqlString = cqlString.replaceAll("\\[\\]", "[word=\".*\"]");
		Pattern pattern = Pattern.compile(" *([\"'=\\|\\&\\[\\]\\(\\)]) *");
		Matcher matcher = pattern.matcher(cqlString);
		if (matcher.matches()) {
		    String match = matcher.group(1);
		    cqlString = cqlString.replaceAll(" *"+match+" *", match);
		}
		return cqlString;
	}
	
	private List<String> splitColumns(String cqlString) {
		List<String> columns = new ArrayList<String>();
		int x = -1;
		
		String newCql = "";
		
		for (int i = 0; i < cqlString.length(); i++) {
			String character = cqlString.substring(i, i+1);
			if (character.equals("[") || character.equals("]") || character.equals("(") || character.equals(")") || character.equals("&") || character.equals("|") || character.equals("\"") || character.equals("'")) {
				String newChar = this.addNestingLevelToCharacter(character);
				newCql = newCql+newChar;
			} else
				newCql = newCql+character;
		}
		
		log("<NEWCQL>"+newCql+"</NEWCQL>");
		
		String[] parts = newCql.split("\n");
		for (int i = 0; i < parts.length; i++) {
			String part = parts[i].trim();
			while (part.endsWith("]"))
				part = part.substring(0, part.length()-1);
//			while (part.endsWith("]") || part.endsWith(")"))
//				part = part.substring(0, part.length()-1);
			if (part.length() > 0) {
				Pattern pattern = Pattern.compile("^([0-9]+%)");
				Matcher matcher = pattern.matcher(part);
				if (matcher.find()) {
					log(part+" matches '^[0-9]+%'");
				    String match = matcher.group(1);
					part = part.replace(match, "").trim();
					if (part.endsWith(")"))
						part = part.substring(0, part.length()-1);
					if (part.length() > 0) {
						log("Adding changed part: "+part);
						x++;
						columns.add(x, part);
					}
				} else {
					log(part+" does not match '^[0-9]+%'");
					String[] subparts = part.split(" ");
					for (int s = 0; s < subparts.length; s++) {
//						if (!subparts[s].equals("(") && !subparts[s].equals("|") && !subparts[s].equals("&")) {
							log("Adding part: "+subparts[s]);
							x++;
							columns.add(x, subparts[s]);
//						}
					}
				}
			}
		}
		
		return columns;
	}
	
	private String addNestingLevelToCharacter(String character) {
		String label = character+"";
		if (character.equals("[") && !this.inside_dq && !this.inside_sq) {
			this.levelUp();
			label = "\n"+String.valueOf(this.level)+"%";
		} else if (character.equals("]") && !this.inside_dq && !this.inside_sq) {
			this.levelDown();
		} else if (character.equals("\"")) {
			if (this.inside_dq)
				this.inside_dq = false;
			else if (!this.inside_sq)
				this.inside_dq = true;
		} else if (character.equals("'")) {
			if (this.inside_sq)
				this.inside_sq = false;
			else if (!this.inside_dq)
				this.inside_sq = true;
		} else if (character.equals("(") && !this.inside_dq && !this.inside_sq) {
			this.levelUp();
			label = "\n"+String.valueOf(this.level)+"%"+character+"\n";
		} else if (character.equals(")") && !this.inside_dq && !this.inside_sq) {
			this.levelDown();
			label = "\n"+character+"\n";
		} else if ((character.equals("&") || character.equals("|")) && !this.inside_dq && !this.inside_sq) {
			label = "\n"+character+"\n";
		}
		return label;
	}
	
	private void levelUp() {
		this.level++;
		if (this.level > this.max)
			this.max = this.level;
	}
	
	private void levelDown() {
		this.level--;
	}
	
	private Map<String,Object> processConnectors(Object field) {
		log("processConnectors(field)");
		Map<String,Object> column = new HashMap<String,Object>();
		String connector = null;
		Object filteredFields = null;
		if (field instanceof String) {
			List<String> fields = new ArrayList<String>();
			fields.add((String) field);
			filteredFields = fields;
		} else {
			@SuppressWarnings("unchecked")
			List<Object> fields = (List<Object>) field;
			if (fields.contains("|")) {
				log("fields contains |");
				connector = "OR";
				filteredFields = this.filterFields(fields, "|");
			} else if (fields.contains("&")) {
				log("fields contains &");
				connector = "AND";
				filteredFields = this.filterFields(fields, "&");
			} else {
				log("fields does not contain | or &");
				filteredFields = fields;
			}
		}
		
		if (connector != null) {
			log("adding connector to column: "+connector);
			column.put("connector", connector);
		}
		column.put("fields", filteredFields);
		return column;
	}
	
	private List<Object> filterFields(List<Object> fields, String connector) {
		log("filterFields(fields,connector)");
		List<Object> keep = new ArrayList<Object>();
		for (int i = 0; i < fields.size(); i++) {
			Object fieldObject = fields.get(i);
			if (fieldObject instanceof String) {
				String field = (String) fieldObject;
				if (field.length() > 0 && !field.equals(connector))
					keep.add(field);
			} else {
				keep.add(fieldObject);
			}
		}
		return keep;
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> processQuantifiers(Map<String, Object> column) {
		log("processQuantifiers(column)");
		if (!column.containsKey("connector"))
			log("1/ column has no connector");
		List<Object> fields = (List<Object>) column.get("fields");
		for (int i = 0; i < fields.size(); i++) {
			Object field = fields.get(i);
			if (field instanceof String) {
				String q = this.getQuantifierFromString((String) fields.get(i));
				if (q != null) {
					column = this.processQuantifier(column, q);
					if (column.containsKey("repeat_of"))
						return column;
				}
			}
		}
		column.put("fields", fields);
		if (!column.containsKey("connector"))
			log("2/ column has no connector");
		return column;
	}
	
	private String getQuantifierFromString(String field) {
		log("getQuantifierFromString(field)");
		Pattern pattern = Pattern.compile("\"]*$");
		Matcher matcher = pattern.matcher(field);
		if (!matcher.matches()) {
			String[] bits = field.split("\"]*");
			return bits[bits.length-1];
		}
		return null;
	}
	
	private Map<String, Object> processQuantifier(Map<String, Object> column, String q) {
		log("processQuantifier(column,q)");
		Pattern pattern = Pattern.compile("^\\$([0-9]+)");
		Matcher matcher = pattern.matcher(q);
		if (matcher.matches()) {
			log("field is repeat");
		    String match = matcher.group(1);
		    q = q.replace("$"+match, "");
		    column.put("repeat_of", Integer.parseInt(match) - 1);
//		    column.remove("fields");
//		    if (column.containsKey("connector"))
//		    	column.remove("connector");
		}
		if (q.trim().length() > 0) {
//			column.put("quantifier", q.trim());
			column.put("from", getQuantifierFrom(q.trim()));
			column.put("to", getQuantifierTo(q.trim()));
		} else {
			column.put("from", 1);
			column.put("to", 1);
		}
		return column;
	}
	
	private Integer getQuantifierFrom(String q) {
		Pattern pattern = Pattern.compile("\\{([0-9]+)(,([0-9]+))*\\}");
		Matcher matcher = pattern.matcher(q);
		if (q.equals("*"))
			return 0;
		else if (q.equals("+"))
			return 1;
		else if (matcher.matches())
			return Integer.valueOf(matcher.group(1));
		
		return 1;
	}
	
	private Integer getQuantifierTo(String q) {
		Pattern pattern = Pattern.compile("\\{([0-9]+)((,)([0-9]+)*)*\\}");
		Matcher matcher = pattern.matcher(q);
		if (q.equals("*") || q.equals("+"))
			return 5;
		else if (matcher.matches()) {
			if (matcher.groupCount() == 4 && matcher.group(4) != null && matcher.group(4).length() > 0)
				return Integer.valueOf(matcher.group(4));
			else if (matcher.groupCount() == 3 && matcher.group(3) != null && matcher.group(3).length() > 0)
				return 5;
			else
				return Integer.valueOf(matcher.group(1));
		}
		
		return 1;
	}

	@SuppressWarnings("unchecked")
	private JSONObject processColumnFields(Map<String, Object> column, Boolean optional) {
		log("processColumnFields(column)");
		JSONObject columnJson = new JSONObject();
		List<Object> fields = (List<Object>) column.get("fields");
		JSONArray fieldsJson = new JSONArray();
		for (int i = 0; i < fields.size(); i++) {
			Object field = fields.get(i);
			if (field instanceof String) {
				JSONObject jsonField = this.processStringToField((String) field);
				if (jsonField.has("field"))
					fieldsJson.put(i, jsonField);
				else if (jsonField.has("repeat_of")) {
					columnJson.put("repeat_of", jsonField.get("repeat_of"));
				}
			} else {
				fieldsJson.put(i, this.processColumnFields((Map<String, Object>) field, false));
			}
		}
		if (!columnJson.has("repeat_of")) {
			columnJson.put("fields", fieldsJson);
			if (column.containsKey("connector"))
				columnJson.put("connector", column.get("connector"));
//			if (column.containsKey("quantifier"))
//				columnJson.put("quantifier", column.get("quantifier"));
		}
		columnJson.put("optional", optional);
		
		return columnJson;
	}

	private JSONObject processStringToField(String fieldString) {
		log("processStringToField(fieldString)");
		JSONObject field = new JSONObject();
			
		if (fieldString == null || fieldString.equals("") || fieldString.equals("word=\"\"")) {
			field.put("field", "WordToken");
		} else {
			String regex1 = "(word|w|lemma|l|pos|p)(!*=)['\"](\\(\\?[ci]\\))*(.+)['\"](\\{[0-9]+,*[0-9]*\\}|\\*|\\+)*";
			String regex2 = "^\\$([0-9]+)$";
			Pattern pattern1 = Pattern.compile(regex1);
			Matcher matcher1 = pattern1.matcher(fieldString);
			Pattern pattern2 = Pattern.compile(regex2);
			Matcher matcher2 = pattern2.matcher(fieldString);
			if (matcher1.find()) {
				String definition = matcher1.group(1);
				String operator = matcher1.group(2);
				String sensitivity = matcher1.group(3);
				String value = matcher1.group(4);
				String quantifier = matcher1.group(5);
				field.put("field", this.definitionToField(definition, value));
				Matcher matcher3 = pattern2.matcher(value);
				if (matcher3.matches()) {
					log("HIER A");
					int matchField = Integer.parseInt(matcher3.group(1));
					field.put("repeat_of", matchField);
				} else {
					field.put("pattern", value);
					field.put("operator", this.operatorToString(operator));
					if (sensitivity != null && sensitivity.length() > 0 && sensitivity.equals("(?c)"))
						field.put("case_sensitive", true);
					if (quantifier != null && quantifier.length() > 0)
						field.put("quantifier", quantifier);
				}
			} else if (matcher2.matches()) {
				log("HIER B");
				int matchField = Integer.parseInt(matcher2.group(1));
//				field.put("field", "WordType");
				field.put("repeat_of", matchField);
			} else {
				field.put("field", "WordType");
				field.put("pattern", fieldString);
			}
		}
		return field;
	}
	
	private String definitionToField(String definition, String value) {
		log("definitionToField(definition,value)");
		if (definition.equals("word") || definition.equals("w"))
			return "WordType";
		else if (definition.equals("lemma") || definition.equals("l"))
			return "Lemma";
		else if (definition.equals("pos") || definition.equals("p")) {
			Pattern pattern = Pattern.compile("^[A-Z]+$");
			Matcher matcher = pattern.matcher(value);
			if (matcher.matches())
				return "PosHead";
			else
				return "PosTag";
		}
		return "WordToken";
	}
	
	private String operatorToString(String operator) {
		if (operator.equals("!="))
			return "not_equal";
		return "equal";
	}
	
}
