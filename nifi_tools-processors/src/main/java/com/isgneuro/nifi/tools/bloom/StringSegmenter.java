package com.isgneuro.nifi.tools.bloom;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.unbescape.html.HtmlEscape;

public class StringSegmenter {
    public static String DEFAULT_TOKENIZE_STR = "\r\n\t[]<>(){}\\\"«»'`.,;!?-+*/^&@$#%_:= ";
    public static int DEFAULT_MIN_TOKEN_LENGTH = 3;
    public static boolean DEFAULT_FILTER_NUMERIC_TOKENS = false;
    ArrayList<String> regexpList = new ArrayList<>();
    private final String tokenizeStr;
    private final boolean filterNumericTokens;
    private final int minTokenLength;
    private final Pattern numPattern = Pattern.compile("-?\\d+(\\.\\d+)?");

    public StringSegmenter(String tokenizeStr, boolean filterNumericTokens, int minTokenLength){
        this.tokenizeStr = tokenizeStr;
        this.minTokenLength = minTokenLength;
        this.filterNumericTokens = filterNumericTokens;
        addDefaultRegexStr();
    }

    public StringSegmenter(){
        this(DEFAULT_TOKENIZE_STR, DEFAULT_FILTER_NUMERIC_TOKENS, DEFAULT_MIN_TOKEN_LENGTH);
    }

    public StringSegmenter(String tokenizeStr){
        this(tokenizeStr, DEFAULT_FILTER_NUMERIC_TOKENS, DEFAULT_MIN_TOKEN_LENGTH);
    }

    public StringSegmenter(String tokenizeStr, boolean filterNumericTokens){
        this(tokenizeStr, filterNumericTokens, DEFAULT_MIN_TOKEN_LENGTH);
    }

    public StringSegmenter(boolean filterNumericTokens, int minTokenLength){
        this(DEFAULT_TOKENIZE_STR, filterNumericTokens, minTokenLength);
    }



    private void addDefaultRegexStr(){
        // word-word
        regexpList.add("[\\p{L}\\p{Digit}]+(\\p{Pd}[\\p{L}\\p{Digit}]+)+");
        // ip
        regexpList.add("\\b(?:[0-9]{1,3}\\.){3}[0-9]{1,3}\\b");
        // email
        regexpList.add("[a-z0-9]+@[a-z]+\\.[a-z]{2,3}");
        // hh:mm with optional leading 0
        regexpList.add("([0-9]|0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]");
        // YYYY-MM-dd or YYYY.MM.dd or YYYY/MM/dd
        regexpList.add("([12]\\d{3}(\\/|-|\\.)(0[1-9]|1[0-2])(\\/|-|\\.)(0[1-9]|[12]\\d|3[01]))");
        // dd-MM-YYYY or dd.MM.YYYY or dd/MM/YYYY
        regexpList.add("(?:(?:31(\\/|-|\\.)(?:0?[13578]|1[02]))\\1|(?:(?:29|30)(\\/|-|\\.)(?:0?[1,3-9]|1[0-2])\\2))(?:(?:1[6-9]|[2-9]\\d)?\\d{2})$|^(?:29(\\/|-|\\.)0?2\\3(?:(?:(?:1[6-9]|[2-9]\\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?:0?[1-9]|1\\d|2[0-8])(\\/|-|\\.)(?:(?:0?[1-9])|(?:1[0-2]))\\4(?:(?:1[6-9]|[2-9]\\d)?\\d{2})");
        // dd-mmm-YYYY or dd/mmm/YYYY or dd.mmm.YYYY
        regexpList.add("(?:(?:31(\\/|-|\\.)(?:0?[13578]|1[02]|(?:Jan|Mar|May|Jul|Aug|Oct|Dec)))\\1|(?:(?:29|30)(\\/|-|\\.)(?:0?[1,3-9]|1[0-2]|(?:Jan|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\\2))(?:(?:1[6-9]|[2-9]\\d)?\\d{2})$|^(?:29(\\/|-|\\.)(?:0?2|(?:Feb))\\3(?:(?:(?:1[6-9]|[2-9]\\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?:0?[1-9]|1\\d|2[0-8])(\\/|-|\\.)(?:(?:0?[1-9]|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep))|(?:1[0-2]|(?:Oct|Nov|Dec)))\\4(?:(?:1[6-9]|[2-9]\\d)?\\d{2})");
    }

    public void addRegexStr(String regexStr) throws PatternSyntaxException {
        Pattern pattern = Pattern.compile(regexStr);
        regexpList.add(regexStr);
    }

    public Set<String> getTokensByRegexStr(String rawStr, String regexStr) throws PatternSyntaxException {
        return getTokensByRegexStr(rawStr, regexStr, 0);
    }

    public Set<String> getTokensByRegexStr(String rawStr, String regexStr, int group) throws PatternSyntaxException {
        String str = HtmlEscape.unescapeHtml(rawStr);
        List<String> tokensList = new ArrayList<>();
        Pattern pattern = Pattern.compile(regexStr);
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            tokensList.add(matcher.group(group));
        }
        return tokensList.stream()
                .map(String::toLowerCase)
                .filter(token -> token.length() >= minTokenLength)
                .filter(token -> !filterNumericTokens || !numPattern.matcher(token).matches())
                .collect(Collectors.toSet());
    }

    public Set<String> parseString(String rawStr){
        String str = HtmlEscape.unescapeHtml(rawStr);
        List<String> tokensList = Collections.list(new StringTokenizer(str, tokenizeStr)).stream()
                .map(token -> ((String) token)).collect(Collectors.toList());
        for (String s : regexpList) {
            Pattern pattern = Pattern.compile(s);
            Matcher matcher = pattern.matcher(str);
            while (matcher.find()) {
                tokensList.add(matcher.group(0));
            }
        }
        return tokensList.stream()
                .map(String::toLowerCase)
                .filter(token -> token.length() >= minTokenLength)
                .filter(token -> !filterNumericTokens || !numPattern.matcher(token).matches())
                .collect(Collectors.toSet());
    }

    public Set<String> parseJSON(String jsonStr) throws JsonProcessingException {
        String str = HtmlEscape.unescapeHtml(jsonStr);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> jsonMap = objectMapper.readValue(str,
                new TypeReference<Map<String,String>>(){});
        List<String> tokensList = new ArrayList<>();
        for(String element:jsonMap.keySet()){
            tokensList.addAll(parseString(element));
        }
        for(String element:jsonMap.values()){
            tokensList.addAll(parseString(element));
        }
        return tokensList.stream()
                .map(String::toLowerCase)
                .filter(token -> token.length() >= minTokenLength)
                .filter(token -> !filterNumericTokens || !numPattern.matcher(token).matches())
                .collect(Collectors.toSet());
    }
}
