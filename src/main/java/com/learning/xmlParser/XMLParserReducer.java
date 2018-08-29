package com.learning.xmlParser;

import com.learning.xmlParser.beans.CD;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class XMLParserReducer extends Reducer<DoubleWritable, CD, DoubleWritable, CD> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<CD> values, Context context) throws IOException, InterruptedException {
        for (CD cd : values)
            context.write(key, cd);
    }
}
