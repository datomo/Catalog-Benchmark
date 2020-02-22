package catalog.db.main;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;


public class CheapSerializer {

    static class Serializer<T extends Serializable> {

        public byte[] serialize( T object ) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                ObjectOutputStream os = new ObjectOutputStream( out );
                os.writeObject( object );
                return out.toByteArray();
            } catch ( IOException e ) {
                e.printStackTrace();
            } finally {
                try {
                    out.close();
                } catch ( IOException e ) {
                    e.printStackTrace();
                }
            }
            return null;

        }


        public T deserialize( byte[] bytes ) {
            ByteArrayInputStream in = new ByteArrayInputStream( bytes );
            try {
                ObjectInputStream is = new ObjectInputStream( in );
                Object object = is.readObject();
                // TODO: find better solution
                return (T) is.readObject();
            } catch ( IOException | ClassNotFoundException e ) {
                e.printStackTrace();
            } finally {
                try {
                    in.close();
                } catch ( IOException e ) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }


    static class SchemaSerializer {

        // https://stackoverflow.com/questions/3736058/java-object-to-byte-and-byte-to-object-converter-for-tokyo-cabinet/3736091
        public static byte[] serialize( SchemaEntry entry ) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                ObjectOutputStream os = new ObjectOutputStream( out );
                os.writeObject( entry );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            return out.toByteArray();
        }


        public static SchemaEntry deserialize( byte[] bytes ) {
            ByteArrayInputStream in = new ByteArrayInputStream( bytes );
            try {
                ObjectInputStream is = new ObjectInputStream( in );
                return (SchemaEntry) is.readObject();
            } catch ( IOException | ClassNotFoundException e ) {
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
