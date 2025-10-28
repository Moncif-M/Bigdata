package edu.ensias.hadoop;

import java.io.IOException;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path nomcomplet = new Path(args[0]);
    if (! fs.exists(nomcomplet)) {
    FSDataOutputStream outStream = fs.create(nomcomplet);
    outStream.writeUTF("Bonjour tout le monde !");
    outStream.writeUTF(args[0]);
    outStream.close();
    }
    fs.close();
}
}
