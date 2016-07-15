package io;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HadoopIO {
	public static void main(String[] args) {
		System.out.println("Start writing file to HDFS...");
		writeToHdfs();

		System.out.println("Start reading file from HDFS...");
		readFromHdfs();
		
		System.out.println("End task");

	}

	private static void writeToHdfs() {
		String localSrc = "/Users/williamzhang/test/1903";
		String dst = "hdfs://137.207.64.66:9000/usr/input/1903.txt";

		try {
			InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
			Configuration conf = new Configuration();

			FileSystem fs = FileSystem.get(URI.create(dst), conf);
			OutputStream out = fs.create(new Path(dst), new Progressable() {
				public void progress() {
					System.out.println("in progress");
				}
			});
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {

		}
	}

	private static void readFromHdfs() {
		String dst = "hdfs://192.168.0.186:9000/usr/input/hello";
		Configuration conf = new Configuration();
		
		try {
			FileSystem fs = FileSystem.get(URI.create(dst), conf);
			FSDataInputStream hdfsInStream = fs.open(new Path(dst));
			
			OutputStream out = new FileOutputStream("/Users/williamzhang/test/helloFromHdfs.txt");
			byte[] ioBuffer = new byte[1024];
			int readLen = hdfsInStream.read(ioBuffer);
			
			while (-1 != readLen) {
				out.write(ioBuffer, 0, readLen);
				readLen = hdfsInStream.read(ioBuffer);
			}
			out.close();
			hdfsInStream.close();
			fs.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

}
