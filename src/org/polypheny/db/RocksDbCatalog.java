package org.polypheny.db;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;


/**
 * Test implementation of a Rocks store, ignores a lot of possible errors etc.
 */
public class RocksDbCatalog {

    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private ColumnFamilyHandle schema;
    private ColumnFamilyHandle schemaChildren;
    private ColumnFamilyHandle table;
    private ColumnFamilyHandle tableChildren;
    private ColumnFamilyHandle column;

    private RocksDB db;


    static {
        RocksDB.loadLibrary();
    }


    public RocksDbCatalog() {
        final String db_path = "rockdb";

        // open DB with two column families
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions() ) );

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schema".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schemaChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tables".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tablesChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "column".getBytes(), new ColumnFamilyOptions() ) );

        this.columnFamilyHandles = columnFamilyHandles;
        final DBOptions options = new DBOptions();
        options.setCreateIfMissing( true );
        options.setCreateMissingColumnFamilies( true );
        try {
            open( db_path, columnFamilyDescriptors, columnFamilyHandles, options );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    private void open( String db_path, List<ColumnFamilyDescriptor> columnFamilyDescriptors, List<ColumnFamilyHandle> columnFamilyHandles, DBOptions options ) throws RocksDBException {
        this.db = RocksDB.open( options, db_path, columnFamilyDescriptors, columnFamilyHandles );

        this.schema = columnFamilyHandles.get( 0 );
        this.schemaChildren = columnFamilyHandles.get( 1 );
        this.table = columnFamilyHandles.get( 2 );
        this.tableChildren = columnFamilyHandles.get( 3 );
        this.column = columnFamilyHandles.get( 4 );
        System.out.println( "came here" );

    }


    public void close() {
        for ( final ColumnFamilyHandle handle : columnFamilyHandles ) {
            handle.close();
        }
        this.db.close();
    }


    public void addSchema( SchemaEntry schema ) {
        try {
            db.put( this.schema, schema.getBytes(), schema.getBytes() );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    public SchemaEntry getSchema( String schema ) {
        try {
            return new SchemaEntry( new String( db.get( this.schema, schema.getBytes() ) ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    public void addTable( String schema, TableEntry table ) {

        // List<String> children = db.get( this.schemaChildren, schema.getBytes() );

    }

}
