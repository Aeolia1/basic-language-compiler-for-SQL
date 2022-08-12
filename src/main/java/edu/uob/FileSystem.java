package edu.uob;

import java.io.*;
import java.util.ArrayList;

public class FileSystem {
    ArrayList<String> dbNamesList = new ArrayList<>(); //list of database name under directory
    ArrayList<ArrayList<String>> allTableList = new ArrayList<>(); //table name list under each database
    ArrayList<ArrayList<Integer>> idList = new ArrayList<>();
    private final File dbDir;
    private String currentDB;



    public FileSystem(File dbDir) throws IOException {
        this.dbDir=dbDir;
    }


    public File getDbDir() {
        return dbDir;
    }



    public void setCurrentDB(String currentDB) {
        this.currentDB = currentDB;
    }



    public String getCurrentDB() {
        return currentDB;
    }




    public int getIdCounter(String tableName, String currentDB){
        int dbIndex = dbNamesList.indexOf(currentDB);
        int tableIndex = allTableList.get(dbIndex).indexOf(tableName);
        return idList.get(dbIndex).get(tableIndex);
    }




    public void setIdList(String tableName, int idCounter){
        int dbIndex = dbNamesList.indexOf(currentDB);
        int tableIndex = allTableList.get(dbIndex).indexOf(tableName);
        idList.get(dbIndex).set(tableIndex,idCounter);
    }




    //While creating the database, initialize a new table and id list. Add a new array to the array list.
    public void addNewDB(String dbName){
        dbNamesList.add(dbName);
        //initialise table and id list for new database
        ArrayList<String>newTableList = new ArrayList<>();
        ArrayList<Integer>newIdList = new ArrayList<>();
        allTableList.add(newTableList);
        idList.add(newIdList);
    }



    public void addNewTable(String tableName){
        int index = dbNamesList.indexOf(currentDB);
        allTableList.get(index).add(tableName);
        idList.get(index).add(0);
    }




    public void dropDB(String dbName){
        if(dbNamesList.contains(dbName)){
            int index = dbNamesList.indexOf(dbName);
            dbNamesList.remove(dbName);
            allTableList.remove(index);
            idList.remove(index);
        }

    }



    public void dropTable(String tableName, String dbName){
        if(dbNamesList.contains(dbName)){
            int dbIndex = dbNamesList.indexOf(dbName);
            int tableIndex = allTableList.get(dbIndex).indexOf(tableName);
            allTableList.get(dbIndex).remove(tableName);
            idList.get(dbIndex).remove(tableIndex);
        }
    }

}
