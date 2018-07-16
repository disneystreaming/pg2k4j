package com.disney.pg2k4j.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SlotMessage {

    protected final int xid;
    protected final List<Change> change;

    @JsonCreator
    public SlotMessage(
            @JsonProperty(value = "xid", required = true) final int xid,
            @JsonProperty(value = "change", required = true) final List<Change> change
    ) {
        this.xid = xid;
        this.change = change;
    }

    public int getXid() {
        return xid;
    }

    public List<Change> getChange() {
        return change;
    }

}
