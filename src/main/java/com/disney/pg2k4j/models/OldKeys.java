package com.disney.pg2k4j.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class OldKeys {
    private final List<String> keytypes;
    private final List<Object> keyvalues;
    private final List<String> keynames;

    @JsonCreator
    public OldKeys(
            @JsonProperty(value = "keytypes", required = true) final List<String> keytypes,
            @JsonProperty(value = "keyvalues", required = true) final List<Object> keyvalues,
            @JsonProperty(value = "keynames", required = true) final List<String> keynames
            ) {
        this.keytypes = keytypes;
        this.keyvalues = keyvalues;
        this.keynames = keynames;
    }

    public List<String> getKeytypes() {
        return keytypes;
    }

    public List<Object> getKeyvalues() {
        return keyvalues;
    }

    public List<String> getKeynames() {
        return keynames;
    }

    @JsonIgnore
    public List<String> getColumnnames() {
        return getKeynames();
    }

    @JsonIgnore
    public List<Object> getColumnvalues() {
        return getKeyvalues();
    }
}
