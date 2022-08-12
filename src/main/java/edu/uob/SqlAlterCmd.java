package edu.uob;

import java.io.File;
import java.io.IOException;

public class SqlAlterCmd extends DBCmd{


    public void addAttribute(String attributeName) throws DBException, IOException {
        String fileName = dbDir + File.separator+ currentDB;
        File fileToOpen = new File(fileName+File.separator+tableName+".tab");
        DBTable newTable = new DBTable();
        //if file is empty, initialize the file with id column
        if(fileToOpen.length()==0){
            attributeList.add(0,"id");
            attributeList.add(attributeName);
            newTable.addAttributeRow(attributeList);
            updateFile(tableName,newTable);
            return;
        }
        //check if attribute name already exists
        readFile(tableName,newTable);
        checkAttribExist(attributeName,newTable);
        newTable.addAttribute(attributeName);
        updateFile(tableName,newTable);
    }



    public void dropAttribute(String attribute) throws DBException, IOException {
        String fileName = dbDir + File.separator+ currentDB;
        File fileToOpen = new File(fileName+File.separator+tableName+".tab");
        DBTable newTable = new DBTable();
        if(fileToOpen.length()==0){
            throw new DBException("Table : " + tableName + " is an empty table.");
        }
        //Check if the file exists
        readFile(tableName,newTable);
        attributeList=newTable.getFirstColumn();
        if(!attributeList.contains(attribute)){
            throw new DBException("Attribute name: " +attribute + " doesn't exist in table: " + tableName);
        }
        newTable.dropAttribute(attribute);
        updateFile(tableName,newTable);
    }

}