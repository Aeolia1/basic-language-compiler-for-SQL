package edu.uob;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBToken {
    private String currentWord;
    private String command;
    ArrayList<String> commandWords = new ArrayList<>();
    private int counter = 0;


    public DBToken(String command) {
        this.command = command;
    }


    // split tokens with spaces
    public void splitCommand() {
        // Use regex to separate spaces and strings containing 2 single quotes
        ArrayList<String> tmpArray = new ArrayList<>();
        Pattern regex = Pattern.compile("'([^']*)'|[^\\s\']+");
        Matcher regexMatcher = regex.matcher(command);
        while (regexMatcher.find()) {
            tmpArray.add(regexMatcher.group());
        }

        // Traverse the divided string,
        // add spaces on both sides of the special symbols,
        // and finally divide according to the spaces
        for (String tempStr : tmpArray) {
            if (tempStr.startsWith("'") && tempStr.endsWith("'")) {
                commandWords.add(tempStr);
            } else {
                tempStr = splitString(tempStr);
                String[] tmpString = tempStr.split("\\s+");
                commandWords.addAll(Arrays.asList(tmpString));
            }
        }
    }



    public String splitString(String tempStr) {
        tempStr = tempStr.replace(";", "\t;\t");
        tempStr = tempStr.replace(")", "\t)\t");
        tempStr = tempStr.replace("(", "\t(\t");
        tempStr = tempStr.replace(",", "\t,\t");
        tempStr = tempStr.replace("!", "\t!\t");
        tempStr = tempStr.replace(">", "\t>\t");
        tempStr = tempStr.replace("<", "\t<\t");
        tempStr = tempStr.replace("=", "\t=\t");
        tempStr = tempStr.replace("*", "\t*\t");
        tempStr = tempStr.trim();
        return tempStr;
    }



    public void nextToken() {
        counter += 1;
    }



    public String getPreviousToken() {
        return commandWords.get(counter - 1);
    }



    public String getCurrentWord() {
        currentWord = commandWords.get(counter);
        return currentWord;
    }



    public boolean isLastWord() {
        int lastIndex = commandWords.size() - 1;
        String cw = commandWords.get(counter); // There is no need to judge after separation;; ghf; these situations do not exist
        return (counter == lastIndex) && (cw.equals(";"));
    }



    public String getOperator() {
        if (commandWords.get(counter + 1).equals("=")) {
            counter++;
            return currentWord + "=";
        }
        return currentWord;
    }
}

