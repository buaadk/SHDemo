package com.test.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 * 上传文件
 */
public class MyFSDataOutputStream {

	protected static void uploadData(FileSystem fileSystem, String FILE_PATH) throws Exception {
		try {
			System.out.println("--------------------开始上传文件---------------------------");
			FSDataOutputStream out = fileSystem.create(new Path(FILE_PATH));
			InputStream in = new FileInputStream(new File("D:/data/test/log.txt"));
			IOUtils.copyBytes(in, out, 1024, true);
			System.out.println("------------------上传HDFS完毕-------------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
