package edu.uob;

public class DBException extends Exception{
    private static final long serialVersionUID = -2629325698018774532L;

    public DBException() {}

    public DBException(String message) {
        super("[ERROR]: " + message);
    }

}
