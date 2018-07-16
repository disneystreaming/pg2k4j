package com.disney.pg2k4j.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class InsertChange extends Change {
    private final List<Object> columnvalues;
    private final List<String> columnnames;
    private final List<String> columntypes;

    @JsonCreator
    public InsertChange(
            @JsonProperty(value = "kind", required = true) final String kind,
            @JsonProperty(value = "columnnames", required = true) final List<String> columnnames,
            @JsonProperty(value = "columntypes", required = true) final List<String> columntypes,
            @JsonProperty(value = "table", required = true) final String table,
            @JsonProperty(value = "columnvalues", required = true) final List<Object> columnvalues,
            @JsonProperty(value = "schema", required = true) final String schema
    ) {
        super(kind, table, schema);
        this.columnvalues = columnvalues;
        this.columnnames = columnnames;
        this.columntypes = columntypes;
    }

    public List<Object> getColumnvalues() {
        return columnvalues;
    }

    public List<String> getColumnnames() {
        return columnnames;
    }

    public List<String> getColumntypes() {
        return columntypes;
    }
}
