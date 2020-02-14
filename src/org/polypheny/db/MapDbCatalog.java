package org.polypheny.db;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.polypheny.db.DbSerialize.ColumnSerializer;
import org.polypheny.db.DbSerialize.ListSerializer;
import org.polypheny.db.DbSerialize.SchemaSerializer;
import org.polypheny.db.DbSerialize.TableSerializer;


/**
 * Database catalog which uses the schemaChildren and tableChildren maps as an index and the specific columns as information holder
 * this should help to reduce access of specific columns and table-/schema-specific requests
 */
public class MapDbCatalog {

    private final DB db;

    private final ConcurrentMap<String, SchemaEntry> schemas;
    private final ConcurrentMap<String, TableEntry> tables;
    private final ConcurrentMap<String, ColumnEntry> columns;

    private final ConcurrentMap<String, List<String>> schemaChildren;
    private final ConcurrentMap<String, List<String>> tableChildren;


    public MapDbCatalog() {
        db = DBMaker
                .memoryDB()
                .closeOnJvmShutdown()
                .make();

        this.schemas = db
                .hashMap( "schemas", Serializer.STRING, new SchemaSerializer() )
                .create();

        this.tables = db
                .hashMap( "tables", Serializer.STRING, new TableSerializer() )
                .create();

        this.columns = db
                .hashMap( "columns", Serializer.STRING, new ColumnSerializer() )
                .create();

        this.schemaChildren = db
                .hashMap( "schemaChildren", Serializer.STRING, new ListSerializer<String>( Serializer.STRING ) )
                .create();

        this.tableChildren = db
                .hashMap( "tableChildren", Serializer.STRING, new ListSerializer<String>( Serializer.STRING ) )
                .create();
    }


    public void addSchema( String name ) throws TableExistsException {
        /*if ( this.schemaChildren.get( name ) != null ) {
            throw new TableExistsException();
        }*/
        this.schemaChildren.put( name, ImmutableList.of() );
        this.schemas.put( name, new SchemaEntry( name ) );
    }


    public void addTable( String schema, String table ) {
        List<String> tables = new ArrayList<>( this.schemaChildren.get( schema ) );
        tables.add( table );
        this.schemaChildren.put( schema, tables );
        this.tableChildren.put( schema + "." + table, ImmutableList.of() );
        this.tables.put( schema + "." + table, new TableEntry( table ) );
    }


    public void addColumn( String schema, String table, String column ) {
        List<String> columns = new ArrayList<>( this.tableChildren.get( schema + "." + table ) );
        columns.add( column );
        this.tableChildren.put( schema + "." + table, columns );
        this.columns.put( schema + "." + table + "." + column, new ColumnEntry( column ) );
    }


    public ColumnEntry getColumn( String schema, String table, String column ) {
        return this.columns.get( schema + "." + table + "." + column );
    }


    class TableExistsException extends Exception {

        final String message = "table already exists in the catalog database";
    }
}
