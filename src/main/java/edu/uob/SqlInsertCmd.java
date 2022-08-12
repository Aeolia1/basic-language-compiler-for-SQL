package edu.uob;


import java.io.File;
import java.io.IOException;


public class SqlInsertCmd extends DBCmd{

    public void insertValues(int idCounter) throws DBException, IOException {
        //check if file is empty
        String tablePath = getTablePath(tableName);
        File filePath = new File(tablePath);
        if(filePath.length()==0){
            throw new DBException("Table: " + tableName + " is empty. Insert Fail.");
        }
        //Report an error if the number of attributes entered is
        // less/more than the attributes on the table
        DBTable newTable = new DBTable();
        readFile(tableName,newTable);
        if(valueList.size() != newTable.getNumberOfColumns()-1){
            throw new DBException("The input values doesn't match the number of attributes");
        }
        //insert value list
        newTable.setIdCounter(idCounter);
        newTable.addRow(valueList);
        updateFile(tableName,newTable);
    }
}

