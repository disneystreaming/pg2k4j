package com.disney.pg2k4j.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class UpdateChange extends InsertChange {

    private final OldKeys oldkeys;

    @JsonCreator
    public UpdateChange(
            @JsonProperty(value = "kind", required = true) final String kind,
            @JsonProperty(value = "columnnames", required = true) final List<String> columnnames,
            @JsonProperty(value = "columntypes", required = true) final List<String> columntypes,
            @JsonProperty(value = "table", required = true) final String table,
            @JsonProperty(value = "columnvalues", required = true) final List<Object> columnvalues,
            @JsonProperty(value = "schema", required = true) final String schema,
            @JsonProperty(value = "oldkeys", required = true) final OldKeys oldkeys
    ) {
        super(kind, columnnames, columntypes, table, columnvalues, schema);
        this.oldkeys = oldkeys;
    }

    public OldKeys getOldkeys() {
        return oldkeys;
    }
}
