package com.disney.pg2k4j.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DeleteChange extends Change {

    private final OldKeys oldkeys;

    @JsonCreator
    public DeleteChange(
            @JsonProperty(value = "kind", required = true) final String kind,
            @JsonProperty(value = "table", required = true) final String table,
            @JsonProperty(value = "schema", required = true) final String schema,
            @JsonProperty(value = "oldkeys", required = true) final OldKeys oldkeys
    ) {
        super(kind,  table, schema);
        this.oldkeys = oldkeys;
    }

    public OldKeys getOldkeys() {
        return oldkeys;
    }

    @Override
    @JsonIgnore
    public List<String> getColumnnames() {
        return oldkeys.getKeynames();
    }

    @Override
    @JsonIgnore
    public List<Object> getColumnvalues() {
        return oldkeys.getKeyvalues();
    }
}
