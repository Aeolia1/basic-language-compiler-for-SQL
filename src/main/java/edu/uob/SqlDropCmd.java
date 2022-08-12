package edu.uob;

import java.io.File;
import java.util.ArrayList;

public class SqlDropCmd extends DBCmd{

    public void checkExist() throws DBException {
        String path = getDbPath(dbName);
        File dbPath = new File(path);
        if(!dbPath.exists()){
            throw new DBException ("Delete Database : " + dbName + " Fail. Target database doesn't exist." );
        }
    }



    public void dropDB() throws DBException {

        String path = getDbPath(dbName);
        File dbPath = new File(path);
        if(!dbList.contains(dbName)){
            File[] tableFiles = dbPath.listFiles();
            for (int i = 0; i < tableFiles.length; i++) {
                int index = tableFiles[i].getName().indexOf('.');
                String tableName = tableFiles[i].getName().substring(0, index);
                dropTable(tableName,dbName);
            }
        }else{
            ArrayList<String> tableNameList = getTableNameList(dbName);
            for (String table : tableNameList){
                dropTable(table,dbName);
            }
        }


        if(!dbPath.delete()){
            throw new DBException ("Delete Database : " + dbName + " Fail." );
        }
    }



    public void dropTable(String tableName, String dbName) throws DBException {
        String path = dbDir+ File.separator+dbName;
        File tablePath = new File(path+File.separator+tableName+".tab");
        if(!tablePath.exists()){
            throw new DBException ("Delete table : " + tableName + " Fail. Target table doesn't exist." );
        }
        if(!tablePath.delete()){
            throw new DBException ("Delete table : " + tableName + " Fail." );
        }
    }
}
