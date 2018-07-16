package com.disney.pg2k4j.models;

public class UnknownColumnNameException extends Exception {

    public UnknownColumnNameException(String columnName) {
        super(String.format("Unknown column name %s", columnName));
    }
}
