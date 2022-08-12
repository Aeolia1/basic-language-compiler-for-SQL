package edu.uob;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DBCmd {
    ArrayList<String> tableList = new ArrayList<>();
    ArrayList<String> conditions = new ArrayList<>();
    ArrayList<ArrayList<String>> allTableList = new ArrayList<>();
    ArrayList<String>dbList = new ArrayList<>();
    ArrayList<String> attributeList = new ArrayList<>();
    ArrayList<String> valueList = new ArrayList<>();
    ArrayList<ArrayList<Integer>> idList = new ArrayList<>();
    ArrayList<String> rowBool = new ArrayList<>();
    ArrayList<String>keys=new ArrayList<>(); //for name value pair, update
    ArrayList<String>values=new ArrayList<>(); //for update
    List<String> list = Arrays.asList(">", "<", ">=", "<=","==","!=","LIKE");
    ArrayList<String> operators = new ArrayList<>(list); //for conditions
    ArrayList<ArrayList<String>> stackArr= new ArrayList<>(); //for conditions push in and out
    String dbName,dbDir,currentDB,tableName,result = "";
    String attributeName,operator,strValue,digitValue,value;

    int counter = 0;



    public void getMulCon(DBTable newTable) throws DBException {
        ArrayList<String>symbolStack = new ArrayList<>(); //for AND /OR
        ArrayList<String>tmpBool; //temp array for each new condition
        for(int i=0;i<conditions.size();i++){
            //If a comparison symbol is encountered, assign values to these three properties
            if(operators.contains(conditions.get(i)) || conditions.get(i).equalsIgnoreCase("like")){
                attributeName = conditions.get(i-1);
                operator = conditions.get(i);
                value = conditions.get(i+1);
            }
            //When AND/OR is encountered, store in the symbolStack at 0 index.
            if(conditions.get(i).equals("AND") || conditions.get(i).equals("OR")){
                symbolStack.add(0,conditions.get(i));
            }
            if(conditions.get(i).equals(")") || conditions.get(i).equals(";")){
                if(value==null){
                    if(symbolStack.get(0).equals("AND")){
                        tmpBool=andCompare(stackArr.get(0),stackArr.get(1));
                        stackArr.remove(0);
                        stackArr.remove(0);
                        stackArr.add(0,tmpBool);
                    }
                    if(symbolStack.get(0).equals("OR")){
                        tmpBool=orCompare(stackArr.get(0),stackArr.get(1));
                        stackArr.remove(0);
                        stackArr.remove(0);
                        stackArr.add(0,tmpBool);
                    }
                    symbolStack.remove(0);
                }
                else{
                    checkTableAttrib(newTable,attributeName);
                    tmpBool=newTable.getConArr(attributeName,value,operator);
                    attributeName = null; //Set value,attributeName and operator to null after producing boolArr
                    operator = null;
                    value = null;
                    stackArr.add(0,tmpBool); //push new boolArr into stack
                }
            }
        }
    }




    public ArrayList<String> andCompare(ArrayList<String> boolOne, ArrayList<String> boolTwo){
        ArrayList<String> tmpArr = new ArrayList<>();
        for(int i=0;i<boolOne.size();i++){
            if(boolOne.get(i).equals("true") && boolTwo.get(i).equals("true")){
                tmpArr.add("true");
            }
            else{
                tmpArr.add("false");
            }
        }
        return tmpArr;
    }




    public ArrayList<String> orCompare(ArrayList<String> boolOne, ArrayList<String> boolTwo){
        ArrayList<String> tmpArr = new ArrayList<>();
        for(int i=0;i<boolOne.size();i++){
            if(boolOne.get(i).equals("false") && boolTwo.get(i).equals("false")){
                tmpArr.add("false");
            }
            else{
                tmpArr.add("true");
            }
        }
        return tmpArr;
    }



    //check if input integer is overflow
    public void checkInt(String integer) throws DBException {
        try{
            Integer.parseInt(integer);
        }catch (NumberFormatException e){
            throw new DBException("Integer is overFlow");
        }
    }



    //check if input float is overflow
    public void checkFloat(String floatNumber) throws DBException {
        try{
            Float.parseFloat(floatNumber);
        }catch (NumberFormatException e){
            throw new DBException("Float is overFlow");
        }
    }



    //Check if the left and right sides of the symbols are correct data types
    // e.g: id > 12,><= the left side of these symbols must be numbers
    public void checkOperands() throws DBException {
        List<String> list = Arrays.asList(">", "<", ">=", "<=");
        ArrayList<String> operators = new ArrayList<>(list);
        if(operators.contains(operator)){
            if(digitValue==null){
                throw new DBException(operator+" must followed by a digit. "+strValue + " is not a digit.");
            }
            digitValue=null;
        }
    }



    //Assign a boolean value to each row of the table against the condition
    //and check whether the attribute name exists in the table
    public void checkTableCon(DBTable newTable,String attribName,String value,String operator) throws DBException {
        checkTableAttrib(newTable,attribName);
        rowBool=newTable.getConArr(attribName,value,operator);
    }



    //check whether the attribute name exists in the table
    public void checkTableAttrib(DBTable newTable,String attribName) throws DBException {
        if(!newTable.getRow(0).contains(attribName)){
            throw new DBException("Attribute name: "+attribName+" doesn't exist in table " + tableName);
        }
    }



    public ArrayList<String> getTableNameList(String dbName) throws DBException { //get table list under a specific database
        String dbPath = getDbPath(dbName);
        File database = new File(dbPath);
        File[] tableFiles = database.listFiles();
        ArrayList<String> tableNameList = new ArrayList<>();
        if(tableFiles==null){
            throw new DBException("Table list is null");
        }
        for (File tableFile : tableFiles) {
            int index = tableFile.getName().indexOf('.');
            String tableName = tableFile.getName().substring(0, index);
            tableNameList.add(tableName);
        }
        return tableNameList;
    }




    public String getDbPath(String dbName){
        return dbDir + File.separator + dbName;
    }



    public String getTablePath(String tableName){
        String dbPath = dbDir + File.separator + currentDB;
        return dbPath+File.separator+tableName+".tab";
    }



    public void checkTableExist(String tableName, String currentDB, String dbDir) throws DBException {
        String path = dbDir+ File.separator + currentDB;
        File tablePath = new File(path+File.separator+tableName+".tab");
        if(!tablePath.exists()){
            throw new DBException ( tableName + " Fail. Target table doesn't exist." );
        }
        this.tableName=tableName;
        this.currentDB=currentDB;
        this.dbDir=dbDir;
    }



    public void checkDbExist(String dbName, String dbDir) throws DBException, IOException {
        this.dbName = dbName;
        this.dbDir = dbDir;
        String dbPath = dbDir + File.separator + dbName;
        File database = new File(dbPath);
        //Determine if database already exists
        if (!database.exists()) {
            throw new DBException("Database name: " + dbName + " does not exist.");
        }
    }



    //Check if attribute name already exists in table
    //if yes, throw ERROR
    public void checkAttribExist(String attributeName,DBTable table) throws DBException {
        attributeList = table.getFirstColumn();
        if(attributeList.contains(attributeName)){
            throw new DBException("Attribute : " + attributeName + " Already exists.");
        }
    }



    public  void checkEmptyFile(String tableName) throws DBException {
        String tablePath = getTablePath(tableName);
        File filePath = new File(tablePath);
        if(filePath.length()==0){
            throw new DBException("Table: " + tableName + " is empty. Insert Fail.");
        }
    }



    public void readFile(String tableName,DBTable table) throws IOException {
        String fileName = dbDir + File.separator+ currentDB;
        File fileToOpen = new File(fileName+File.separator+tableName+".tab");
        try{
            FileReader reader = new FileReader(fileToOpen);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String str = bufferedReader.readLine();
            while (str!=null){
                String[] tmpString = str.split("\\s+");
                ArrayList<String> words = new ArrayList<>(Arrays.asList(tmpString));
                table.table.add(words);
                str = bufferedReader.readLine();
            }
            reader.close();
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }
    }



    public void updateFile(String tableName, DBTable table) throws IOException{
        String filePth = dbDir+File.separator + currentDB;
        File fileToOpen = new File(filePth+File.separator+tableName+".tab");
        try{
            FileWriter writer = new FileWriter(fileToOpen);
            for (int i=0; i<table.getNumberOfRows(); i++){
                writer.write(table.rowToString(table.getRow(i)));
                writer.write("\n");
            }
            writer.flush();
            writer.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}


