package org.polypheny.db;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;


public class CheapSerializer {


    static class SchemaSerializer {

        public static byte[] serialize( SchemaEntry entry ) {
            DataOutput2 out = new DataOutput2();
            try {
                out.writeUTF( entry.getName() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            return out.copyBytes();
        }


        public static SchemaEntry deserialize( byte[] bytes ) {
            DataInput2 in = new DataInput2.ByteArray( bytes );
            try {
                String name = in.readUTF();
                return new SchemaEntry( name );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            return null;
        }
    }


    static class TableSerializer {

        public static byte[] serialize( TableEntry entry ) {
            DataOutput2 out = new DataOutput2();
            try {
                out.writeUTF( entry.getSchema() );
                out.writeUTF( entry.getName() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            return out.copyBytes();
        }


        public static TableEntry deserialize( byte[] bytes ) {
            DataInput2 in = new DataInput2.ByteArray( bytes );
            try {
                return new TableEntry( in.readUTF(), in.readUTF() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            return null;
        }
    }


    static class ColumnSerializer {

        public static byte[] serialize( ColumnEntry entry ) {
            DataOutput2 out = new DataOutput2();
            try {
                out.writeUTF( entry.getSchema() );
                out.writeUTF( entry.getTable() );
                out.writeUTF( entry.getName() );

            } catch ( IOException e ) {
                e.printStackTrace();
            }
            return out.copyBytes();
        }


        public static ColumnEntry deserialize( byte[] bytes ) {
            DataInput2 in = new DataInput2.ByteArray( bytes );
            try {
                return new ColumnEntry( in.readUTF(), in.readUTF(), in.readUTF() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            return null;
        }
    }


    static class StringListSerializer {

        public static byte[] serialize( List<String> list ) throws IOException {
            DataOutput2 out = new DataOutput2();

            for ( String e : list ) {
                out.writeUTF( e );
            }
            out.writeInt( list.size() );

            return out.copyBytes();

        }


        public static List<String> deserialize( byte[] bytes ) throws IOException {
            DataInput2 in = new DataInput2.ByteArray( bytes );

            int size = in.readInt();
            List<String> list = new ArrayList<>();
            for ( int i = 0; i < size; i++ ) {
                list.add( in.readUTF() );
            }
            return Collections.unmodifiableList( list );
        }
    }
}
