package com.learning.xmlParser;

import com.learning.xmlParser.beans.CD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class XMLParserRunner {

    public int run(String input, String output) throws Exception {
        String tempDir = "tempDirForJob";

        Configuration configuration = new Configuration();
        configuration.set("START_TAG_KEY", "<CD>");
        configuration.set("END_TAG_KEY", "</CD>");

        Job job = new Job(configuration, "Parse XML");

        job.setJarByClass(XMLParserRunner.class);
        job.setMapperClass(XMLParserMapper.class);
        job.setReducerClass(XMLParserReducer.class);
        job.setCombinerClass(XMLParserReducer.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(CD.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        boolean isSuccessfully;
        try {
            isSuccessfully = job.waitForCompletion(true);
        } finally {
            FileSystem fs = FileSystem.get(configuration);
            fs.delete(new Path(tempDir));
            fs.rename(new Path(output + "/part-r-00000"), new Path(tempDir));
            fs.delete(new Path(output), true);
            fs.rename(new Path(tempDir), new Path(output));
        }

        return isSuccessfully ? 1 : 0;
    }
}
