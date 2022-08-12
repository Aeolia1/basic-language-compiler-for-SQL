package edu.uob;

import java.io.IOException;

public class SqlUpdateCmd extends DBCmd{

    public void updateTable() throws DBException, IOException {
        checkEmptyFile(tableName);
        //check if table contains keys required
        //open table and check if each key exists on the table
        DBTable newTable = new DBTable();
        readFile(tableName,newTable);
        for(String attrib:keys){
            checkTableAttrib(newTable,attrib);
        }

        //for single condition
        newTable.setKeyIndexList(keys);
        if(conditions.size()==4){
            checkTableCon(newTable,attributeName,value,operator);
            newTable.conArr=rowBool;
        }else{ //for multi-condition
            getMulCon(newTable);
            newTable.conArr=stackArr.get(0);

        }
        newTable.updateTable(values);
        updateFile(tableName, newTable);
    }
}
