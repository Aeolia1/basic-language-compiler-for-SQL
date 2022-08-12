package edu.uob;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DBTable {
    ArrayList<ArrayList<String>> table = new ArrayList<>();
    ArrayList<ArrayList<String>> subTable = new ArrayList<>();
    ArrayList<String> attributeList = new ArrayList<>();
    ArrayList<String> conArr = new ArrayList<>();
    ArrayList<Integer> keyIndexList = new ArrayList<>();
    List<String> list = Arrays.asList(">", "<", ">=", "<=");
    ArrayList<String> operators = new ArrayList<>(list);
    private int index;
    private int idCounter;
    private String result = "";



    public DBTable(){}

    public void addAttributeRow(ArrayList<String> attributeList){
        table.add(0,attributeList);
    }


    public String rowToString(ArrayList<String> row){
        String rowString = "";
        for(String cell : row){
            rowString = rowString.concat(cell+"\t");
        }
        return  rowString;
    }

    public int getNumberOfRows(){
        return table.size();
    }


    public  void setIdCounter(int counter){
        idCounter=counter;
    }


    public ArrayList<String> getFirstColumn(){
        return table.get(0);
    }


    public ArrayList<String> getRow(int index){
        return table.get(index);
    }


    public void addAttribute(String attributeName){
        table.get(0).add(attributeName);
    }



    public void dropAttribute(String attributeName){
        setAttributeList();
        index = attributeList.indexOf(attributeName);
        deleteColumn(index);
    }



    public void updateTable(ArrayList<String> valueList){
        //find the index of each key from the first row of the table
        ArrayList<Integer>indexArr = new ArrayList<>();
        for(int i=1; i<table.size();i++){
            if(conArr.get(i-1).equals("true")){
                indexArr.add(i);
            }
        }

        //use keyIndex and conArrIndex to update data listed in the value list.
        int rowIndex,colIndex;
        String value;
        for (Integer integer : indexArr) { //row index
            rowIndex = integer;
            for (int j = 0; j < keyIndexList.size(); j++) {  //col index
                value = valueList.get(j);
                colIndex = keyIndexList.get((j));
                table.get(rowIndex).set(colIndex, value);
            }
        }

    }



    public void setKeyIndexList(ArrayList<String> keys){
        for(String key: keys){
            index=table.get(0).indexOf(key);
            keyIndexList.add(index);
        }
    }



    public void setAttributeList(){
        attributeList=table.get(0);
    }



    public void deleteColumn(int index){
        for(int i=0; i< table.size(); i++){
            if(table.get(i).size()>index){
                table.get(i).remove(index);
            }
        }
    }



    public ArrayList<String> getAttribData(String AttribName){
        ArrayList<String>dataList = new ArrayList<>();
        index = table.get(0).indexOf(AttribName);
        for(int i=0; i< table.size(); i++){
            dataList.add(table.get(i).get(index));
        }
        return dataList;

    }



    public void addRow(ArrayList<String> values){
        String id = String.valueOf(idCounter);
        values.add(0,id);
        table.add(values);
    }


    public int getNumberOfColumns(){
        return table.get(0).size();
    }


    public String getSelectAll(){
        for (ArrayList<String> row : table) {
            result = result.concat(rowToString(row) + "\n");
        }
        return  result;
    }


    public ArrayList<String> getConArr(String attribName,String value,String operator) throws DBException {
        ArrayList<String>tmpBool = new ArrayList<>();
        index = table.get(0).indexOf(attribName);
        for(int i=1; i<table.size();i++){
            if(operators.contains(operator)){
                boolean isInt = value.matches("[-+]?[0-9]*");
                boolean isFloat = value.matches("[-+]?[0-9]*\\.[0-9]*");
                if (isInt){
                    int number = Integer.parseInt(table.get(i).get(index));
                    int valueNum = Integer.parseInt(value);
                    intCompare(operator,number,valueNum,tmpBool);
                }
                else if(isFloat){
                    float number =Float.parseFloat(table.get(i).get(index));
                    float valueNum = Float.parseFloat(value);
                    floatCompare(operator,number,valueNum,tmpBool);
                }
                else{
                    throw new DBException(operator+" must followed by a digit. "+value + " is not a digit.");
                }

            }
            else if(operator.equals("==")){
                if(table.get(i).get(index).equals(value)){
                    tmpBool.add("true");
                }else{
                    tmpBool.add("false");
                }
            }
            else if(operator.equals("!=")){
                if(!table.get(i).get(index).equals(value)){
                    tmpBool.add("true");
                }else{
                    tmpBool.add("false");
                }
            }
            else if(operator.equalsIgnoreCase("LIKE")){
                if(table.get(i).get(index).contains(value)){
                    tmpBool.add("true");
                }else{
                    tmpBool.add("false");
                }
            }
        }
        return tmpBool;
    }




    public void intCompare(String operator, int number, int valueNum, ArrayList<String> tmpBool){

        if(operator.equals(">")){
            if(number > valueNum){
                tmpBool.add("true");
            }
            else{
                tmpBool.add("false");
            }
        }
        if(operator.equals(">=")){
            if(number >= valueNum){
                tmpBool.add("true");
            }
            else{
                tmpBool.add("false");
            }
        }
        if(operator.equals("<")){
            if(number < valueNum){
                tmpBool.add("true");
            }
            else{
                tmpBool.add("false");
            }
        }
        if(operator.equals("<=")){
            if(number <= valueNum){
                tmpBool.add("true");
            }
            else{
                tmpBool.add("false");
            }
        }
    }



    public void floatCompare(String operator, float number, float valueNum, ArrayList<String>tmpBool){

        if(operator.equals(">")){
            if(number > valueNum){
                tmpBool.add("true");
            }
            else{
                tmpBool.add("false");
            }
        }
        if(operator.equals(">=")){
            if(number >= valueNum){
                tmpBool.add("true");
            }
            else{
                tmpBool.add("false");
            }
        }
        if(operator.equals("<")){
            if(number < valueNum){
                tmpBool.add("true");
            }
            else{
                tmpBool.add("false");
            }
        }
        if(operator.equals("<=")){
            if(number <= valueNum){
                tmpBool.add("true");
            }
            else{
                tmpBool.add("false");
            }
        }
    }




    public String getConAll(){
        result = result.concat(rowToString(table.get(0))+"\n");
        for(int i=1; i<table.size();i++){
            if(conArr.get(i-1).equals("true")){
                result = result.concat(rowToString(table.get(i))+"\n");
            }
        }
        return result;
    }



    public String getConSelect(){
        //add first row to the top
        subTable.add(table.get(0));
        for(int i=1; i<table.size();i++){
            if(conArr.get(i-1).equals("true")){
                subTable.add(table.get(i));
            }
        }
        ArrayList<Integer> indexArr = new ArrayList<>();
        for(String attribute : attributeList){
            index = table.get(0).indexOf(attribute);
            indexArr.add(index);
        }
        for(ArrayList<String> row: subTable){
            for(int i=0; i<indexArr.size();i++){
                result= result.concat(row.get(indexArr.get(i))+"\t");
            }
            result = result.concat("\n");
        }
        return result;
    }



    public void deleteCon(){
        subTable.add(table.get(0));
        for(int i=1; i<table.size();i++){
            if(conArr.get(i-1).equals("false")){
                subTable.add(table.get(i));
            }
        }
        table=subTable;

    }



    public String getSelectedPart(){
        // firstly, get the index of each attribute from the attribute row
        ArrayList<Integer> indexArr = new ArrayList<>();
        for(String attribute : attributeList){
            index = table.get(0).indexOf(attribute);
            indexArr.add(index);
        }

        // next, loop through every row, save all the result that matched the index inside the index array into a result string.
        for (ArrayList<String> strings : table) {
            for (int j = 0; j < strings.size(); j++) {
                if(strings.size()>j){
                    index = strings.indexOf(strings.get(j));
                    if (indexArr.contains(index)) {
                        result = result.concat(strings.get(j) + "\t");
                    }
                }

            }
            result = result.concat("\n");
        }
        return result;
    }


    public Integer getMaxId(){
        if(table.size()==1 || table.get(0).get(0).equals("")){
            return 0;
        }
        index=Integer.parseInt(table.get(table.size()-1).get(0));
        return index;
    }
}
