package nl.whitelab.neo4j.util;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexValueChecker {
	private final static Pattern doublePattern = Pattern.compile("[\\x00-\\x20]*[+-]?(((((\\p{Digit}+)(\\.)?((\\p{Digit}+)?)([eE][+-]?(\\p{Digit}+))?)|(\\.((\\p{Digit}+))([eE][+-]?(\\p{Digit}+))?)|(((0[xX](\\p{XDigit}+)(\\.)?)|(0[xX](\\p{XDigit}+)?(\\.)(\\p{XDigit}+)))[pP][+-]?(\\p{Digit}+)))[fFdD]?))[\\x00-\\x20]*");
	private final static Pattern integerPattern = Pattern.compile("^[0-9]+$");
	private final static String[] booleanValues = new String[]{"true", "false", "yes", "no"};
	
	public static boolean isDouble(String s) {
	    Matcher m = doublePattern.matcher(s);
	    return m.matches();
	}
	
	public static boolean isInteger(String s) {
	    Matcher m = integerPattern.matcher(s);
	    return m.matches();
	}
	
	public static boolean isBoolean(String s) {
		return Arrays.asList(booleanValues).contains(s.toLowerCase());
	}

}
