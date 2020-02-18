package org.polypheny.db;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;


/**
 * Test implementation of a Rocks store, ignores a lot of possible errors etc.
 */
public class RocksDbCatalog implements DbCatalog {

    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private ColumnFamilyHandle schemas;
    private ColumnFamilyHandle schemaChildren;
    private ColumnFamilyHandle tables;
    private ColumnFamilyHandle tableChildren;
    private ColumnFamilyHandle columns;

    private RocksDB db;

    private final String db_path = "test";


    static {
        RocksDB.loadLibrary();
    }


    // TODO: renaming map identifiers
    public RocksDbCatalog() {
        final String db_path = "rockdb";

        // open DB with two column families
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions() ) );

        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schemas".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "schemaChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tables".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "tablesChildren".getBytes(), new ColumnFamilyOptions() ) );
        columnFamilyDescriptors.add( new ColumnFamilyDescriptor( "columns".getBytes(), new ColumnFamilyOptions() ) );

        this.columnFamilyHandles = columnFamilyHandles;
        final DBOptions options = new DBOptions();
        options.setCreateIfMissing( true );
        options.setCreateMissingColumnFamilies( true );
        try {
            open( columnFamilyDescriptors, columnFamilyHandles, options );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
    }


    private void open( List<ColumnFamilyDescriptor> columnFamilyDescriptors, List<ColumnFamilyHandle> columnFamilyHandles, DBOptions options ) throws RocksDBException {
        this.db = RocksDB.open( options, db_path, columnFamilyDescriptors, columnFamilyHandles );

        this.schemas = columnFamilyHandles.get( 0 );
        this.schemaChildren = columnFamilyHandles.get( 1 );
        this.tables = columnFamilyHandles.get( 2 );
        this.tableChildren = columnFamilyHandles.get( 3 );
        this.columns = columnFamilyHandles.get( 4 );

    }


    @Override
    public void close() {
        for ( final ColumnFamilyHandle handle : columnFamilyHandles ) {
            handle.close();
        }
        this.db.close();
    }


    @Override
    public void addSchema( SchemaEntry schema ) {
        try {
            byte[] bytes = CheapSerializer.StringListSerializer.serialize( new ArrayList<>() );
            db.put( this.schemaChildren, schema.getName().getBytes(), bytes );
            db.put( this.schemas, schema.getName().getBytes(), CheapSerializer.SchemaSerializer.serialize( schema ) );
        } catch ( RocksDBException | IOException e ) {
            e.printStackTrace();
        }
    }


    @Override
    public void addTable( TableEntry table ) {
        String schema = table.getSchema();
        try {
            byte[] bytes = db.get( this.schemaChildren, schema.getBytes() );
            List<String> list = CheapSerializer.StringListSerializer.deserialize( bytes );
            List<String> tables = new ArrayList<>( list );
            tables.add( table.getName() );
            db.put( this.schemaChildren, schema.getBytes(), CheapSerializer.StringListSerializer.serialize( tables ) );
            db.put( this.tables, (schema + "." + table.getName()).getBytes(), CheapSerializer.TableSerializer.serialize( table ) );
            db.put( this.tableChildren, (schema + "." + table.getName()).getBytes(), CheapSerializer.StringListSerializer.serialize( new ArrayList<>() ) );
        } catch ( RocksDBException | IOException e ) {
            e.printStackTrace();
        }

    }


    @Override
    public void addColumn( ColumnEntry column ) {
        String schema = column.getSchema();
        String table = column.getTable();
        try {
            byte[] bytes = db.get( this.tableChildren, (schema + "." + table).getBytes() );
            List<String> columns = new ArrayList<>( CheapSerializer.StringListSerializer.deserialize( bytes ) );
            columns.add( column.getName() );
            db.put( this.tableChildren, (schema + "." + table).getBytes(), CheapSerializer.StringListSerializer.serialize( columns ) );
            db.put( this.columns, (schema + "." + table + "." + column.getName()).getBytes(), CheapSerializer.ColumnSerializer.serialize( column ) );
        } catch ( RocksDBException | IOException e ) {
            e.printStackTrace();
        }
    }


    @Override
    public SchemaEntry getSchema( String schema ) {
        try {
            return CheapSerializer.SchemaSerializer.deserialize( db.get( this.schemas, schema.getBytes() ) );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public TableEntry getTable( String schema, String table ) {
        try {
            byte[] bytes = db.get( this.tables, (schema + "." + table).getBytes() );
            return CheapSerializer.TableSerializer.deserialize( bytes );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public ColumnEntry getColumn( String schema, String table, String column ) {
        try {
            byte[] bytes = db.get( this.columns, (schema + "." + table + "." + column).getBytes() );
            return CheapSerializer.ColumnSerializer.deserialize( bytes );
        } catch ( RocksDBException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public List<String> getSchemaNames() {
        RocksIterator iter = this.db.newIterator( this.schemas );
        iter.seekToFirst();
        List<String> names = new ArrayList<>();
        while ( iter.isValid() ) {
            names.add( new String( iter.key(), StandardCharsets.UTF_8 ) );
            iter.next();
        }
        return names;
    }


    @Override
    public List<String> getTableNames() {
        return null;
    }


    @Override
    public List<String> getColumnNames() {
        return null;
    }


    @Override
    public List<SchemaEntry> getSchemas() {
        return null;
    }


    @Override
    public List<TableEntry> getTables() {
        return null;
    }


    @Override
    public List<ColumnEntry> getColumns() {
        return null;
    }


}
