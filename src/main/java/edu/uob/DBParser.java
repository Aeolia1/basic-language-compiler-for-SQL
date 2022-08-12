package edu.uob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DBParser {
    DBToken token;
    FileSystem fileSystem;
    private String currentWord;
    private static String currentDB; //current database name
    private String dbDir;
    private String command;




    public DBParser(String command, FileSystem fileSystem) throws DBException, IOException {
        this.fileSystem=fileSystem;
        this.command = command;
        token = new DBToken(command);
        token.splitCommand();
        currentDB=fileSystem.getCurrentDB();
        this.dbDir=fileSystem.getDbDir().getPath();
        if (!command.endsWith(";")) {
            throw new DBException("Semi colon missing at end of line");
        }
        parseCommand();
    }

    public void parseCommand() throws DBException, IOException {
        currentWord = token.getCurrentWord().toUpperCase();
        switch (currentWord) {
            case "USE" -> {
                token.nextToken();
                parseUse();
            }
            case "CREATE" -> {
                token.nextToken();
                parseCreate();
            }
            case "DROP" -> {
                token.nextToken();
                parseDrop();
            }
            case "ALTER" -> {
                token.nextToken();
                parseAlter();
            }
            case "INSERT" -> {
                token.nextToken();
                parseInsert();
            }
            case "SELECT" -> {
                token.nextToken();
                parseSelect();
            }
            case "JOIN" -> {
                token.nextToken();
                parseJoin();
            }
            case "DELETE" -> {
                token.nextToken();
                parseDelete();
            }
            case "UPDATE" -> {
                token.nextToken();
                parseUpdate();
            }
            default -> throw new DBException(currentWord + " is an invalid command type!");
        }
    }

    // USE
    public void parseUse() throws DBException, IOException {
        SqlUseCmd inputUse = new SqlUseCmd();
        dataBaseName();
        inputUse.checkDbExist(currentWord,dbDir);//use required database name,directory, database list
        fileSystem.setCurrentDB(currentWord);
        currentDB = currentWord;
        token.nextToken();
        checkLastWord();
        inputUse.addAllExist(fileSystem.allTableList,fileSystem.dbNamesList,fileSystem.idList);
    }



    // CREATE
    public void parseCreate() throws DBException, IOException {
        SqlCreateCmd inputCreate = new SqlCreateCmd();
        currentWord = token.getCurrentWord();
        if (currentWord.equalsIgnoreCase("DATABASE")) {
            token.nextToken();
            createDatabase(inputCreate);
        } else if (currentWord.equalsIgnoreCase("TABLE")) {
            token.nextToken();
            createTable(inputCreate);
        } else {
            throw new DBException(currentWord + " is an invalid to CREATE! \n valid create types: TABLE or DATABASE.");
        }
    }



    public void createDatabase(SqlCreateCmd inputCreate) throws DBException {
        dataBaseName();
        inputCreate.createDB(currentWord,dbDir);
        fileSystem.addNewDB(currentWord);
        token.nextToken();
        checkLastWord();
    }


    public void createTable(SqlCreateCmd create) throws DBException, IOException {
        tableName();
        create.tableName=currentWord;
        token.nextToken();
        if(token.getCurrentWord().equals(";")){
            create.createEmptyTable(currentWord,currentDB,dbDir); // create empty table
            fileSystem.addNewTable(currentWord);
            return;
        }
        if (!token.getCurrentWord().equals("(")) {
            throw new DBException("Expect a ( after table name.");
        }
        token.nextToken();
        attributeList(create); //need to input new attributes for first row of the table file;
        if (!token.getCurrentWord().equals(")")) {
            throw new DBException("Expect a )");
        }
        token.nextToken();
        checkLastWord();
        create.createTable(create.tableName,currentDB,dbDir);
        fileSystem.addNewTable(create.tableName);
    }



    // DROP-------------------------------
    public void parseDrop() throws DBException {
        SqlDropCmd inputDrop = new SqlDropCmd();
        inputDrop.dbDir=dbDir;
        currentWord = token.getCurrentWord();
        if (currentWord.equalsIgnoreCase("DATABASE")) {
            token.nextToken();
            dataBaseName();
            inputDrop.dbName = currentWord; //required directory, dbName
            inputDrop.checkExist();
            inputDrop.dropDB();
            fileSystem.dropDB(currentWord);
        } else if (currentWord.equalsIgnoreCase("TABLE")) {
            token.nextToken();
            tableName();
            inputDrop.dropTable(currentWord,currentDB);
            fileSystem.dropTable(currentWord,currentDB);
        } else {
            throw new DBException(currentWord + " is an invalid drop type;\nValid drop type: DATABASE or TABLE.");
        }
        token.nextToken();
        checkLastWord();
    }



    // ALTER---------------------------
    public void parseAlter() throws DBException, IOException {
        SqlAlterCmd inputAlter = new SqlAlterCmd();
        currentWord = token.getCurrentWord();
        if (!currentWord.equalsIgnoreCase("TABLE")) {
            throw new DBException(currentWord + " is an invalid command for Alter. Should be TABLE.");
        }
        token.nextToken();
        tableName();
        inputAlter.checkTableExist(currentWord,currentDB,dbDir);
        token.nextToken();
        alterationType();
        token.nextToken();
        attributeName();
        if(token.getPreviousToken().equalsIgnoreCase("ADD")){
            inputAlter.addAttribute(currentWord);
        }
        else{
            inputAlter.dropAttribute(currentWord);
        }
        token.nextToken();
        checkLastWord();
    }


    public void alterationType() throws DBException {
        currentWord = token.getCurrentWord().toUpperCase();
        if (currentWord.equals(";")) {
            throw new DBException("An alteration type is missing here.");
        } else if (!currentWord.equals("ADD") && !currentWord.equals("DROP")) {
            throw new DBException(currentWord + " is an invalid alteration type.");
        }
    }



    // INSERT--------------------------------------------
    public void parseInsert() throws DBException, IOException {
        SqlInsertCmd inputInsert = new SqlInsertCmd();
        currentWord = token.getCurrentWord();
        if (!currentWord.equalsIgnoreCase("INTO")) {
            throw new DBException(currentWord + " is an invalid INSERT Type.");
        }
        token.nextToken();
        tableName();
        inputInsert.checkTableExist(currentWord,currentDB,dbDir); //check and input required value to cmd
        token.nextToken();
        currentWord = token.getCurrentWord();
        if (!currentWord.equalsIgnoreCase("VALUES")) {
            throw new DBException(currentWord + " is an invalid command for INSERT INTO. Expect 'VALUES'.");
        }
        token.nextToken();
        if (!token.getCurrentWord().equals("(")) {
            throw new DBException("( is missing here.");
        }
        token.nextToken();
        if (token.getCurrentWord().equals(")")) {
            throw new DBException("Value is missing between ()");
        }
        valueList(inputInsert);
        if (!token.getCurrentWord().equals(")")) {
            throw new DBException("Expect a )");
        }
        token.nextToken();
        checkLastWord();
        int idCounter = fileSystem.getIdCounter(inputInsert.tableName,currentDB)+1;
        inputInsert.insertValues(idCounter);
        fileSystem.setIdList(inputInsert.tableName,idCounter);
    }



    public void valueList(DBCmd inputCmd) throws DBException {
        value(inputCmd);
        inputCmd.valueList.add(currentWord);
        token.nextToken();
        if (token.getCurrentWord().equals(",")) {
            token.nextToken();
            valueList(inputCmd);
        }
    }



    public void value(DBCmd inputCmd) throws DBException {
        currentWord = token.getCurrentWord().toUpperCase();
        if (isBoolean(currentWord) || currentWord.equals("NULL")) {
            return;
        }
        currentWord = token.getCurrentWord();
        boolean isInt = currentWord.matches("[-+]?[0-9]*");
        boolean isFloat = currentWord.matches("[-+]?[0-9]*\\.[0-9]*");

        if (currentWord.startsWith("'") && currentWord.length() > 1) {
            if (!currentWord.endsWith("'")) {
                throw new DBException("A close single quote is missing.");
            }
            stringLiteral();
            return;
        }

        if(isFloat){
            inputCmd.digitValue = currentWord;
            inputCmd.checkFloat(currentWord); //check if input float us overflow
        }
        else if (isInt) {
            inputCmd.digitValue = currentWord;
            inputCmd.checkInt(currentWord); //check if input integer is overflow
        }
        else{
            throw new DBException(currentWord + " is an invalid value.");
        }
    }



    public void stringLiteral() throws DBException {
        if (currentWord.length() == 2) {
            currentWord="";
            return;
        }
        charLiteral();
        //remove head and tail characters' '
        currentWord=currentWord.substring(1, currentWord.length() - 1);
    }



    public void charLiteral() throws DBException {
        for (int i = 1; i < currentWord.length() - 1; i++) {
            if (!Character.isSpaceChar(currentWord.charAt(i)) && !Character.isLetter(currentWord.charAt(i))
                    && !isSymbol(currentWord.charAt(i))) {
                throw new DBException(currentWord.charAt(i) + " is an invalid character for string literal.");
            }
        }
    }



    // SELECT------------------------------------
    public void parseSelect() throws DBException, IOException {
        SqlSelectCmd inputSelect = new SqlSelectCmd();
        wildAttributeList(inputSelect);
        currentWord = token.getCurrentWord();
        if (!currentWord.equalsIgnoreCase("FROM")) {
            throw new DBException(currentWord + " is an invalid command. should be 'FROM'.");
        }
        token.nextToken();
        tableName();
        inputSelect.checkTableExist(currentWord,currentDB,dbDir);
        token.nextToken();
        currentWord = token.getCurrentWord();
        if(currentWord.equals(";")){
            command = inputSelect.getSelectedPart();
        }
        if (currentWord.equalsIgnoreCase("WHERE")) {
            token.nextToken();
            if (token.getCurrentWord().equals(";")) {
                throw new DBException("condition is required after WHERE.");
            }
            condition(inputSelect);
            token.nextToken();
            checkLastWord();
            inputSelect.conditions.add(";");
            command = inputSelect.getConPart();
        } else if (!currentWord.equals(";")) {
            throw new DBException(currentWord + " is invalid. 'WHERE' is required here.");
        }
    }



    public void condition(DBCmd inputCmd) throws DBException {
        currentWord = token.getCurrentWord();
        if (currentWord.equals("(")) {
            inputCmd.conditions.add("("); //enter the ( to the stack
            token.nextToken();
            if (token.getCurrentWord().equals(")")) {
                throw new DBException("Condition is required between ( ).");
            }
            condition(inputCmd);
            token.nextToken();
            if (!token.getCurrentWord().equals(")")) {
                throw new DBException(token.getCurrentWord() + "A close ) is missing.");
            }
            inputCmd.conditions.add(")");  //enter the ')' to the stack
            token.nextToken();
            currentWord = token.getCurrentWord().toUpperCase();
            if (!currentWord.equals("AND") && !currentWord.equals("OR")) {
                throw new DBException(currentWord + " is invalid. AND or OR is required after ).");
            }
            inputCmd.conditions.add(currentWord); // add AND/OR to the stack
            token.nextToken();
            if (!token.getCurrentWord().equals("(")) {
                throw new DBException("( is required after AND or OR");
            }
            inputCmd.conditions.add("("); // enter second (
            token.nextToken();
            condition(inputCmd);
            token.nextToken();
            if (!token.getCurrentWord().equals(")")) {
                throw new DBException("Expect a )");
            }
            inputCmd.conditions.add(")"); // enter second ')'

        } else if (isPlainText(currentWord)) {
            inputCmd.attributeName=currentWord;
            inputCmd.conditions.add(currentWord);
            token.nextToken();
            operator();
            inputCmd.operator=currentWord;
            inputCmd.conditions.add(currentWord);
            token.nextToken();
            value(inputCmd);
            inputCmd.value=currentWord;
            inputCmd.checkOperands();
            inputCmd.conditions.add(currentWord);
        } else {
            throw new DBException(currentWord + " is not a correct condition");
        }
    }



    public void operator() throws DBException {
        currentWord = token.getCurrentWord();
        List<String> list = Arrays.asList("==", ">", "<", ">=", "<=", "!=", "LIKE");
        ArrayList<String> operators = new ArrayList<>(list);
        String operate = token.getOperator();
        if (!operators.contains(operate) && !currentWord.equalsIgnoreCase("like")) {
            throw new DBException(operate + " is an invalid operator.");
        }
        currentWord = operate;
    }



    public void wildAttributeList(DBCmd inputCmd) throws DBException {
        currentWord = token.getCurrentWord();
        if (currentWord.equals("*")) {
            inputCmd.attributeList.add(currentWord);
            token.nextToken();
            return;
        }
        attributeList(inputCmd);
    }



    public void attributeList(DBCmd inputCmd) throws DBException {
        attributeName();
        inputCmd.attributeList.add(currentWord);
        token.nextToken();
        if (token.getCurrentWord().equals(",")) {
            token.nextToken();
            attributeList(inputCmd);
        }
    }



    // JOIN----------------------------
    public void parseJoin() throws DBException, IOException {
        SqlJoinCmd inputJoin = new SqlJoinCmd();
        tableName();
        inputJoin.checkTableExist(currentWord,currentDB,dbDir);
        inputJoin.tableList.add(currentWord); //add table 1 into table list
        token.nextToken();
        currentWord = token.getCurrentWord();
        if (!currentWord.equalsIgnoreCase("AND")) {
            throw new DBException(currentWord + " is invalid. Expect AND.");
        }
        token.nextToken();
        tableName();
        inputJoin.checkTableExist(currentWord,currentDB,dbDir);
        inputJoin.tableList.add(currentWord); //add table 2 into table list
        token.nextToken();
        currentWord = token.getCurrentWord();
        if (!currentWord.equalsIgnoreCase("ON")) {
            throw new DBException(currentWord + " is invalid. Expect ON.");
        }
        token.nextToken();
        attributeName();
        inputJoin.attributeList.add(currentWord); //add attribute name 1
        token.nextToken();
        currentWord = token.getCurrentWord();
        if (!currentWord.equalsIgnoreCase("AND")) {
            throw new DBException(currentWord + " is invalid. Expect AND.");
        }
        token.nextToken();
        attributeName();
        inputJoin.attributeList.add(currentWord); //add attribute name 2
        token.nextToken();
        checkLastWord();
        command = inputJoin.joinTables();
    }



    // DELETE--------------------------
    public void parseDelete() throws DBException, IOException {
        SqlDeleteCmd inputDelete = new SqlDeleteCmd();
        if (!token.getCurrentWord().equalsIgnoreCase("FROM")) {
            throw new DBException(token.getCurrentWord() + " is invalid. Expect a FROM. ");
        }
        token.nextToken();
        tableName();
        inputDelete.checkTableExist(currentWord,currentDB,dbDir); //check if table exists
        token.nextToken();
        if (!token.getCurrentWord().equalsIgnoreCase("WHERE")) {
            throw new DBException(token.getCurrentWord() + " is invalid. Expect a WHERE. ");
        }
        token.nextToken();
        condition(inputDelete);
        token.nextToken();
        checkLastWord();
        inputDelete.conditions.add(";");
        inputDelete.deleteCondition();

    }



    // UPDATE-----------------------------------
    public void parseUpdate() throws DBException, IOException {
        SqlUpdateCmd inputUpdate = new SqlUpdateCmd();
        tableName();
        inputUpdate.checkTableExist(currentWord,currentDB,dbDir);
        token.nextToken();
        if (!token.getCurrentWord().equalsIgnoreCase("SET")) {
            throw new DBException(token.getCurrentWord() + " is invalid. Expect a SET. ");
        }
        token.nextToken();
        nameValueList(inputUpdate);
        if (!token.getCurrentWord().equalsIgnoreCase("WHERE")) {
            throw new DBException(token.getCurrentWord() + " is invalid. Expect a WHERE. ");
        }
        token.nextToken();
        condition(inputUpdate);
        token.nextToken();
        checkLastWord();
        inputUpdate.conditions.add(";");
        inputUpdate.updateTable();
    }



    public void nameValueList(DBCmd inputCmd) throws DBException {
        nameValuePair(inputCmd);
        token.nextToken();
        if (token.getCurrentWord().equals(",")) {
            token.nextToken();
            nameValueList(inputCmd);
        }
    }



    public void nameValuePair(DBCmd inputCmd) throws DBException {
        attributeName();
        inputCmd.keys.add(currentWord); //add keys to key list
        token.nextToken();
        if (!token.getCurrentWord().equals("=")) {
            throw new DBException("Expect =");
        }
        token.nextToken();
        value(inputCmd);
        inputCmd.values.add(currentWord); //add values to value list
    }



    public void checkLastWord() throws DBException {
        if (!token.isLastWord()) {
            throw new DBException(token.getCurrentWord() + " is invalid. Only 1 ; is required at the end. ");
        }
        return;
    }



    public void tableName() throws DBException {
        currentWord = token.getCurrentWord().toLowerCase();
        if (currentWord.equals(";")) {
            throw new DBException("A table name is missing here.");
        }
        if (!isPlainText(currentWord)) {
            throw new DBException(currentWord + " is an invalid table name, table name should be plaintext.");
        }
    }



    public void dataBaseName() throws DBException {
        currentWord = token.getCurrentWord().toLowerCase();
        if (currentWord.equals(";")) {
            throw new DBException("A database name is missing here.");
        }
        if (!isPlainText(currentWord)) {
            throw new DBException(currentWord + "is an invalid database name.");
        }
    }



    public void attributeName() throws DBException {
        currentWord = token.getCurrentWord();
        if (currentWord.equals(";") || currentWord.equals(")")) {
            throw new DBException("An attribute name is missing here.");
        }
        if (!isPlainText(currentWord)) {
            throw new DBException(currentWord + " is an invalid attribute name. This should be a plaintext.");
        }
    }



    public boolean isPlainText(String token) {
        char[] chars = token.toCharArray();
        for (char c : chars) {
            if (!Character.isLetterOrDigit(c)) {
                return false;
            }
        }
        return true;
    }



    public boolean isSymbol(char c) {
        if ((c > 32 && c < 48) || (c > 57 && c < 65) || (c > 90 && c < 97) || (c > 122 && c < 127)) {
            return c != '\'' && c != '|' && c != '"';
        } else {
            return false;
        }
    }



    public boolean isBoolean(String token) {
        return token.equals("TRUE") || token.equals(("FALSE"));
    }



    public String query(DBServer server){
        return "[OK] Thanks for your message: \n" + command;
    }

}
