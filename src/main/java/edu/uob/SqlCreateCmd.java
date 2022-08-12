package edu.uob;
import java.io.File;
import java.io.IOException;


public class SqlCreateCmd extends DBCmd{


    public void createDB(String dbName, String dbDir) throws DBException {
        String path = dbDir+File.separator+dbName;
        File dir = new File(path);
        if(dir.exists()){
            throw new DBException ("Create directory: " + dbName + " Fail. Target directory already exists." );
        }
        if(!dir.mkdirs()){
            throw new DBException("Create Directory: " + dbName + " Fail!");
        }

    }



    //1. Check whether the file exists;
    //2. Generate an empty file
    //3. Add the newly created table to the tableList of the filesystem.
    public void createEmptyTable(String tableName, String currentDB, String dbDir) throws DBException, IOException {
        String path = dbDir+File.separator+currentDB;
        File table = new File(path+File.separator+tableName+".tab");
        if(currentDB==null){
            throw new DBException("The system cannot find the path specified");
        }
        if(table.exists()){
            throw new DBException("Create file: " + tableName + " Fail. File already exists.");
        }
        if(!table.createNewFile()){
            throw new DBException("Create file: " + tableName + " Fail!");
        }
        this.tableName=tableName;
        this.currentDB=currentDB;
        this.dbDir=dbDir;

    }




    public void createTable(String tableName, String currentDB, String dbDir) throws DBException, IOException {
        createEmptyTable(tableName, currentDB, dbDir);
        attributeList.add(0,"id");
        DBTable newTable = new DBTable();
        newTable.addAttributeRow(attributeList);
        updateFile(tableName,newTable);
    }

}
