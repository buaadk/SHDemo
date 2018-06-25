package com.test.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 读取文件
 */
public class MyFSDataInputStream extends FSDataInputStream {

	public MyFSDataInputStream(InputStream in) {
		super(in);
	}

	public static void downloadData(FileSystem fileSystem, String FILE_PATH) throws Exception {
		try {
			System.out.println("--------------------开始读取文件---------------------------");
			FSDataInputStream in = fileSystem.open(new Path(FILE_PATH));

			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String content = null;
			int pointer = 0;
			while ((content = br.readLine()) != null) {
				// 将文本打印到控制台
				System.out.println("读取第" + (++pointer) + "行信息如下：" + content);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected static void makeDir(FileSystem fileSystem, String DIR_PATH) throws IOException {
		fileSystem.mkdirs(new Path(DIR_PATH));
	}
}
