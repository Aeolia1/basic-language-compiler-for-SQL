package edu.uob;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class SqlUseCmd extends DBCmd{


    public void addAllExist(ArrayList<ArrayList<String>>allTables, ArrayList<String> dbNamesList, ArrayList<ArrayList<Integer>> counterList) throws DBException, IOException {
        allTableList = allTables;
        dbList = dbNamesList;
        idList = counterList;
        currentDB = dbName;
        String dbPath = dbDir + File.separator + dbName;
        File database = new File(dbPath);

        //Determine whether there is this file in the current dbNameList
        //If not: Assign the file and its tables in the root directory to
        // the dbNameList in the file system and the corresponding table list
        if (!dbList.contains(dbName)) {

            //It does not exist in the database list, but it exists in the path. Add this database to dbList
            dbList.add(dbName);

            //Add all tables under this database to the table list
            File[] tableFiles = database.listFiles();
            ArrayList<String> tableList = new ArrayList<>();
            ArrayList<Integer> countList = new ArrayList<>();
            for (int i = 0; i < tableFiles.length; i++) {
                int index = tableFiles[i].getName().indexOf('.');
                String tableName = tableFiles[i].getName().substring(0, index);
                tableList.add(tableName);
                if (tableFiles[i].length() == 0) {
                    countList.add(0);
                } else {
                    DBTable newTable = new DBTable();
                    readFile(tableName, newTable);
                    countList.add(newTable.getMaxId());
                }
            }
            allTableList.add(tableList);
            idList.add(countList);
        }
    }

}

