package edu.uob;

import java.io.IOException;
import java.util.ArrayList;

public class SqlJoinCmd extends DBCmd {


    public String joinTables() throws DBException, IOException {

        //1. warning when table is empty
        for(String table: tableList){
            checkEmptyFile(table);
        }

        //2.check if the attribute names exist in the corresponding table.
        String attribOne = attributeList.get(0);
        String attribTwo = attributeList.get(1);
        DBTable tableOne = new DBTable();
        DBTable tableTwo = new DBTable();
        readFile(tableList.get(0),tableOne);
        readFile(tableList.get(1),tableTwo);
        if(!tableOne.getFirstColumn().contains(attribOne)){
            throw new DBException(attributeList.get(0)+" doesn't exist in the table " + tableList.get(0));
        }
        if(!tableTwo.getFirstColumn().contains(attribTwo)){
            throw new DBException(attributeList.get(1)+" doesn't exist in the table " + tableList.get(1));
        }

        //3.Find the list of corresponding attributes in the table
        ArrayList<String> dataOne;
        ArrayList<String> dataTwo;
        dataOne = tableOne.getAttribData(attribOne);
        dataTwo = tableTwo.getAttribData(attribTwo);

        //4.Delete the corresponding attribute column, as well as the id column of each table
        tableOne.dropAttribute(attribOne);
        if(tableOne.getRow(0).contains("id")){
            tableOne.dropAttribute("id");
        }
        tableTwo.dropAttribute(attribTwo);
        if(tableTwo.getRow(0).contains("id")){
            tableTwo.dropAttribute("id");
        }

        //5.match in order
        result = result.concat("id"+"\t"+tableOne.rowToString(tableOne.getRow(0))+tableTwo.rowToString(tableTwo.getRow(0))+"\n");
        for(int i=1;i<tableOne.table.size();i++){
            for(int j=1; j<tableTwo.table.size();j++){
                if(dataOne.get(i).equals(dataTwo.get(j))){
                    counter+=1;
                    result=result.concat(counter+"\t"+tableOne.rowToString(tableOne.getRow(i))+tableTwo.rowToString(tableTwo.getRow(j))+"\n");
                }
            }
        }
        return result;
    }
}

