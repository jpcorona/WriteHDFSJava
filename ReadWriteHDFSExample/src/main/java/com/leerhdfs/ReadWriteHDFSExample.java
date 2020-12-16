package com.leerhdfs;

//import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class ReadWriteHDFSExample {

    public static void main(String[] args) throws IOException {

    	String localsrc = args[0];
    	String destinosrc = args[1];
    	
    	InputStream in = new BufferedInputStream(new FileInputStream(localsrc));
    	Configuration conf = new Configuration();
    	
    	FileSystem fs = FileSystem.get(URI.create(destinosrc), conf);
    	
    	//Progressable ir viendo aumento 10%, 20%, 30%
    	OutputStream out = fs.create(new Path(destinosrc), new Progressable() {
			
			public void progress() {
				System.out.println(".");
				
			}
    	});
    	
    	IOUtils.copyBytes(in ,out, 4096, true);
    }

}
