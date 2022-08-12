package edu.uob;

import java.io.IOException;

public class SqlSelectCmd extends DBCmd{


    public String getSelectedPart() throws DBException, IOException {
        //check if file is empty
        checkEmptyFile(tableName);
        DBTable newTable = new DBTable();
        readFile(tableName,newTable);
        newTable.attributeList = attributeList;
        //if attribute is *
        if (attributeList.contains("*")){
            result = newTable.getSelectAll();
            return result;
        }
        //if it has specific attributes
        //1. check if the attribute actually exist in the table
        for(String attribute: attributeList){
            if(!newTable.getFirstColumn().contains(attribute)){
                throw new DBException(attribute+ " doesn't exist in the table.");
            }
        }
        result = newTable.getSelectedPart();
        return result;
    }



    public String getConPart() throws DBException, IOException {
        checkEmptyFile(tableName);
        //open table
        DBTable newTable = new DBTable();
        readFile(tableName,newTable);
        newTable.attributeList = attributeList;

        //check condition and get updated condition array
        if(conditions.size()==4){ //for single condition;
            checkTableCon(newTable,attributeName,value,operator);
        }else{
            getMulCon(newTable); //for multi-condition
            rowBool=stackArr.get(0);
        }

        newTable.conArr=rowBool;
        if (attributeList.contains("*")){

            result=newTable.getConAll();
        }
        else{
            for(String attribute: attributeList){
                if(!newTable.getFirstColumn().contains(attribute)){
                    throw new DBException(attribute+ " doesn't exist in the table.");
                }
            }
            result=newTable.getConSelect();
        }
        return result;
    }
}
