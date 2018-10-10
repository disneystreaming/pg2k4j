/*******************************************************************************
 Copyright 2018 Disney Streaming Services

 Licensed under the Apache License, Version 2.0 (the "Apache License")
 with the following modification; you may not use this file except in
 compliance with the Apache License and the following modification to it:
 Section 6. Trademarks. is deleted and replaced with:

 6. Trademarks. This License does not grant permission to use the trade
 names, trademarks, service marks, or product names of the Licensor
 and its affiliates, except as required to comply with Section 4(c) of
 the License and to reproduce the content of the NOTICE file.

 You may obtain a copy of the Apache License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the Apache License with the above modification is
 distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied. See the Apache License for the specific
 language governing permissions and limitations under the Apache License.

 ******************************************************************************/

package com.disney.pg2k4j.models;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "kind", visible = true)
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
            @JsonProperty(value = "kind", required = true)
            final String kindInput,
            @JsonProperty(value = "table", required = true)
            final String tableInput,
            @JsonProperty(value = "schema", required = true)
            final String schemaInput
    ) {
        this.kind = kindInput;
        this.table = tableInput;
        this.schema = schemaInput;
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

    public Object getValueForColumn(final String columnName)
            throws UnknownColumnNameException {
        int columnIndex = getColumnnames().indexOf(columnName);
        if (columnIndex != -1) {
            return getColumnvalues().get(columnIndex);
        } else {
            throw new UnknownColumnNameException(columnName);
        }
    }
}
