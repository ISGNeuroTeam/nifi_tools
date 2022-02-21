package com.isgneuro.etl.nifi.processors;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.stream.JsonParser;

import static javax.json.stream.JsonParser.Event.VALUE_NULL;


public class StringSegmenter {
    public static Set<String> parse(String rawStr) {
        Set<String> result = new HashSet<>();
        String[] majorSegments = (rawStr + " ").split("(?<=\r)|(?<=\n)|(?<=\\s)|(?<=\t)|(?<=\\[)|(?<=\\])|(?<=\\<)|(?<=\\>)|(?<=\\()|(?<=\\))|(?<=\\{)|(?<=\\})|(?<=\\|)|(?<=!)|(?<=;)|(?<=‘)|(?<=\")|(?<= )|(?<=,)");
        String[] minorSegments;
        for (String major : majorSegments) {
            if (major.length() > 1) {
                minorSegments = major.split("(?<=/)|(?<=:)|(?<==)|(?<=@)|(?<=\\.)|(?<=-)|(?<=\\$)|(?<=#)|(?<=%)|(?<=\\\\)|(?<=_)");
                StringBuilder currentSegment = new StringBuilder();
                for (String minor : minorSegments) {
                    if (minor.length() > 1) {
                        currentSegment.append(minor);
                        result.add(currentSegment.substring(0, currentSegment.length() - 1));
                        result.add(minor.substring(0, minor.length() - 1));
                    }
                }
            }
        }
        return result.stream().filter(t -> t.length() > 2).map(t -> t.toLowerCase()).collect(Collectors.toSet());
    }

    public static Set<String> parseJson(String rawStr) {
        Set<String> res = new HashSet<>();
        final StringReader sr = new StringReader(rawStr);
        try {
            final JsonParser parser = Json.createParser(sr);
            List<String> pathStack = new ArrayList<>();
            while (parser.hasNext() ) {
                JsonParser.Event event = parser.next();
                switch (event) {
                    case START_ARRAY:
                        pathStack.add("{}");
                        res.add(pathStack.stream().skip(1).collect(Collectors.joining()));
                        break;
                    case END_ARRAY:
                        pathStack.remove(pathStack.size() - 1);
                        pathStack.remove(pathStack.size() - 1);
                        break;
                    case START_OBJECT:
                        pathStack.add(".");
                        break;
                    case END_OBJECT:
                        pathStack.remove(pathStack.size() - 1);
                        if(pathStack.size() > 0 && !pathStack.get(pathStack.size() - 1).equals("{}"))
                            pathStack.remove(pathStack.size() - 1);
                        break;
                    case KEY_NAME:
                        pathStack.add(parser.getString());
                        res.add(pathStack.stream().skip(1).collect(Collectors.joining()));
                        break;
                    case VALUE_FALSE:
                    case VALUE_TRUE:
                    case VALUE_NUMBER:
                    case VALUE_STRING:
                    case VALUE_NULL:
                        String value;
                        if(event.equals(VALUE_NULL))
                            value = "null";
                        else
                            value = parser.getString();
                        res.add(pathStack.stream().skip(1).collect(Collectors.joining()) + "=" + value);
                        if(value.length() > 2) res.add(value);
                        res.addAll(parse(value));
                        pathStack.remove(pathStack.size() - 1);
                        break;
                }
            }
        } catch (Exception e) {
            System.out.println("Parsing Error:" + e);
        }

        sr.close();
        return res;
    }
}
