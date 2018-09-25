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

 *******************************************************************************/

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
