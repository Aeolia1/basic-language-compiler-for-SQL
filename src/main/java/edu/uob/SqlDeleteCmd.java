package edu.uob;

import java.io.IOException;

public class SqlDeleteCmd extends DBCmd{

    public void deleteCondition() throws IOException, DBException {
        checkEmptyFile(tableName);
        //open table
        DBTable newTable = new DBTable();
        readFile(tableName,newTable);

        //check condition and get updated condition array
        if(conditions.size()==4){ //for single condition.
            attributeList=newTable.getFirstColumn();
            checkTableCon(newTable,attributeName,value,operator);
            newTable.conArr=rowBool;
        }
        else{
            getMulCon(newTable); //for multi-conditions
            newTable.conArr=stackArr.get(0);
        }
        newTable.deleteCon();
        updateFile(tableName,newTable);
    }
}
