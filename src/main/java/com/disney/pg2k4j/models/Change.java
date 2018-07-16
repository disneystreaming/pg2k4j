package com.disney.pg2k4j.models;

import com.fasterxml.jackson.annotation.*;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "kind", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = InsertChange.class, name = "insert"),
        @JsonSubTypes.Type(value = UpdateChange.class, name = "update"),
        @JsonSubTypes.Type(value = DeleteChange.class, name = "delete")
})
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public abstract class Change {
    private final String kind;
    private final String table;
    private final String schema;

    @JsonCreator
    public Change(
            @JsonProperty(value = "kind", required = true) final String kind,
            @JsonProperty(value = "table", required = true) final String table,
            @JsonProperty(value = "schema", required = true) final String schema
    ) {
        this.kind = kind;
        this.table = table;
        this.schema = schema;
    }

    public String getKind() {
        return kind;
    }

    public String getTable() {
        return table;
    }

    public String getSchema() {
        return schema;
    }

    public abstract List<String> getColumnnames();

    public abstract List<Object> getColumnvalues();

    public Object getValueForColumn(String columnName) throws UnknownColumnNameException {
        int columnIndex = getColumnnames().indexOf(columnName);
        if (columnIndex != -1) {
            return getColumnvalues().get(columnIndex);
        }
        else {
            throw new UnknownColumnNameException(columnName);
        }
    }
}
