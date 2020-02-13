package org.polypheny.db;


import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;


public class SchemaEntry extends CatalogEntry {

    @Getter
    private final ImmutableList<TableEntry> tables;


    public SchemaEntry( String name, List<TableEntry> tables ) {
        super( name );
        this.tables = ImmutableList.copyOf( tables );
    }
}
