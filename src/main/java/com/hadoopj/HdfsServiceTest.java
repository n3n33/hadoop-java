package com.hadoopj;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
        "classpath:config/spring/context/context-hadoop.xml"
})
public class HdfsServiceTest {
    private static final Logger logger = LoggerFactory.getLogger((HdfsServiceTest.class));

    @Autowired
    public void testDoTest() throws Exception{
        FileSystem hdfs = null;
        try {
            Path filePath = new Path("/user/yenakwon");
            logger.info("filePath.uri={}",filePath.toUri());
            hdfs = FileSystem.get(filePath.toUri(), hdfs.getConf());

            if (hdfs.exists(filePath)){
                logger.info("read file parh={}", filePath);
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(filePath),"utf-8"));
                String line = null;
                do {
                    line = br.readLine();
                    logger.info("   line={}",line);
                }
                while (line != null);
                br.close();
            }
            else {
                logger.info("create new file path={}", filePath);
                FSDataOutputStream out = hdfs.create(filePath, false);
                out.write("한글생성테스트.".getBytes(StandardCharsets.UTF_8));
                out.flush();
                out.close();
            }
        }finally {
            IOUtils.closeStream(hdfs);
        }
    }
}
