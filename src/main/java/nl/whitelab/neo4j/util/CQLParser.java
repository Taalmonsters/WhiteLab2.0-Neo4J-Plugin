package nl.whitelab.neo4j.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONArray;
import org.json.JSONObject;

public class CQLParser {
	private boolean doLogging = true;
	private int index = 0;
	private boolean inside_dq = false;
	private boolean inside_sq = false;
	
	public CQLParser() {}
	
	@SuppressWarnings("unchecked")
	public JSONObject CQLtoJSON(String cqlString) {
		log("CQLtoJSON("+cqlString+")");
		cqlString = normalizeCQL(cqlString);
		log("normalized = "+cqlString);
		Object list = buildList(cqlString, 0);
		log(list);
		
		JSONObject columns = new JSONObject();
		
		int c = 0;
		for (Object obj : (List<Object>) list) {
			if (obj instanceof String) {
				String str = (String) obj;
				Pattern pattern = Pattern.compile("(\\*|\\+|\\{[0-9]+(,([0-9]+))*\\})");
				Matcher matcher = pattern.matcher(str);
				if (matcher.matches()) {
					String quantifier = matcher.group(1);
					JSONObject column = columns.getJSONObject(String.valueOf(c));
					if (quantifier.equals("*"))
						column.put("optional", true);
					else if (quantifier.equals("+"))
						column.put("quantifier", "{1,}");
					else
						column.put("quantifier", quantifier);
					
					columns.put(String.valueOf(c), column);
				}
			} else if (obj instanceof List) {
				c++;
				JSONObject column = nestedListToColumn((List<Object>) obj);
				columns.put(String.valueOf(c), column);
			}
		}
		
		return columns;
	}

	private Object buildList(String nestedList, int level) {
	    List<Object> list = new ArrayList<Object>();

	    while (index < nestedList.length()) {
	        char c = nestedList.charAt(index++);

	        if ((c == '[' || c == '(') && !inside_sq && !inside_dq) {
	        	if (index == 1 || list.size() > 0) {
	        		log("CHAR: "+Character.toString(c)+" --> ADD NESTED LIST");
	        		list.add(buildList(nestedList, level+1));
	        	} else
	        		log("CHAR: "+Character.toString(c)+" --> DO NOTHING");
	        } else if ((c == ']' || c == ')') && !inside_sq && !inside_dq) {
	        	if (level > 0) {
	        		log("CHAR: "+Character.toString(c)+" --> BREAK");
		        	break;
	        	} else
	        		log("CHAR: "+Character.toString(c)+" --> DO NOTHING");
	        } else if ((c == '|' || c == '&') && !inside_sq && !inside_dq) {
	        	log("CHAR: "+Character.toString(c)+" --> ADD TO NEW ELEMENT");
        		list.add(Character.toString(c));
	        } else {
	        	
	        	if (c == '"')
	        		if (inside_dq)
	        			inside_dq = false;
	        		else
	        			inside_dq = true;
	        	
	        	else if (c == '\'')
	        		if (inside_sq)
	        			inside_sq = false;
	        		else
	        			inside_sq = true;
	        	
	        	if (list.size() > 0 && list.get(list.size()-1) instanceof String && !((String) list.get(list.size()-1)).equals("&") && !((String) list.get(list.size()-1)).equals("|")) {
		        	log("CHAR: "+Character.toString(c)+" --> ADD TO PREVIOUS ELEMENT");
	        		list.set(list.size()-1, (String) list.get(list.size()-1)+Character.toString(c));
	        	} else {
	        		log("CHAR: "+Character.toString(c)+" --> ADD TO NEW ELEMENT");
	        		list.add(Character.toString(c));
	        	}
	        }
	    }

	    return list;
	}

	private void log(Object msg) {
		if (doLogging)
			System.out.println("*** CQLParser LOG: "+msg);
	}

	@SuppressWarnings("unchecked")
	private JSONObject nestedListToColumn(List<Object> list) {
		JSONObject column = new JSONObject();
		JSONArray fields = new JSONArray();
		
		for (Object obj : list) {
			if (obj instanceof String) {
				String str = (String) obj;
				String regex1 = "^\\$([0-9]+)$";
				String regex2 = "(word|w|lemma|l|pos|p)(!*=)['\"](\\(\\?[ci]\\))*(.+)['\"](\\{[0-9]+,*[0-9]*\\}|\\*|\\+)*";
				Pattern pattern1 = Pattern.compile(regex1);
				Matcher matcher1 = pattern1.matcher(str);
				Pattern pattern2 = Pattern.compile(regex2);
				Matcher matcher2 = pattern2.matcher(str);
				
				if (str.equals("&") || str.equals("|"))
					column.put("connector", str);
				else if (matcher1.matches()) {
					column.put("repeat_of", matcher1.group(1));
					fields = new JSONArray();
					break;
				} else if (matcher2.matches())
					fields.put(stringToField(matcher2.group(1), matcher2.group(2), matcher2.group(3), matcher2.group(4), matcher2.group(5)));
				
			} else
				fields.put(nestedListToColumn((List<Object>) obj));
		}

		if (fields.length() > 0)
			column.put("fields", fields);
		
		return column;
	}

	private String normalizeCQL(String cqlString) {
		cqlString = cqlString.replaceAll("\\[\\]", "[word=\".*\"]").replaceAll("\\)\\|\\(", "|").replaceAll("\\)\\&\\(", "&");
		Pattern pattern = Pattern.compile(" *([\"'=\\|\\&\\[\\]\\(\\)]) *");
		Matcher matcher = pattern.matcher(cqlString);
		if (matcher.matches()) {
		    String match = matcher.group(1);
		    cqlString = cqlString.replaceAll(" *"+match+" *", match);
		}
		return cqlString;
	}
	
	private String operatorToString(String operator) {
		if (operator.equals("!="))
			return "not_equal";
		return "equal";
	}

	private JSONObject stringToField(String type, String operator, String sensitivity, String value, String quantifier) {
		JSONObject field = new JSONObject();
		field.put("field", typeToField(type, value));
		field.put("pattern", StringEscapeUtils.escapeJava(value));
		field.put("operator", operatorToString(operator));
		if (sensitivity != null && sensitivity.length() > 0 && sensitivity.equals("(?c)"))
			field.put("case_sensitive", true);
		if (quantifier != null && quantifier.length() > 0)
			field.put("quantifier", quantifier);
		return field;
	}
	
	private String typeToField(String definition, String value) {
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

}
