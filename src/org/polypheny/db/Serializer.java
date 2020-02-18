package org.polypheny.db;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;


public class Serializer {


    public static byte[] toByteArray( CatalogEntry entry ) {
        // https://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream out = new ObjectOutputStream( bos );
            out.writeObject( entry );
            out.flush();
            return bos.toByteArray();
        } catch ( IOException e ) {
            e.printStackTrace();
        } finally {

        }
        return null;

    }


    // caller needs to handle casting for now
    // TODO: close streams
    public static Object fromByteArray( byte[] bytes ) {
        ByteArrayInputStream bis = new ByteArrayInputStream( bytes );
        try ( ObjectInput in = new ObjectInputStream( bis ) ) {
            return in.readObject();
        } catch ( IOException | ClassNotFoundException e ) {
            e.printStackTrace();
        } finally {
        }
        return null;

    }


    public static byte[] toByteArray( List<String> list ) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream( bos );
            oos.writeObject( list );
            oos.flush();
            return bos.toByteArray();

        } catch ( IOException e ) {
            e.printStackTrace();
        }
        return new byte[0];
    }
}
