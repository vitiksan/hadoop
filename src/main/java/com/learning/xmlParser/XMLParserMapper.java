package com.learning.xmlParser;

import com.learning.xmlParser.beans.CD;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class XMLParserMapper extends Mapper<LongWritable, Text, DoubleWritable, CD> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("[<>]");
        Text title = new Text();
        Text artist = new Text();
        Text country = new Text();
        Text company = new Text();
        DoubleWritable price = new DoubleWritable();
        IntWritable year = new IntWritable();

        for (int i = 0; i < split.length; i++) {
            switch (split[i]) {
                case "TITLE":
                    title = new Text(split[i + 1]);
                    break;
                case "ARTIST":
                    artist = new Text(split[i + 1]);
                    break;
                case "COUNTRY":
                    country = new Text(split[i + 1]);
                    break;
                case "COMPANY":
                    company = new Text(split[i + 1]);
                    break;
                case "PRICE":
                    price = new DoubleWritable(Double.parseDouble(split[i + 1]));
                    break;
                case "YEAR":
                    year = new IntWritable(Integer.parseInt(split[i + 1]));
                    break;
                default:
                    break;
            }
        }

        context.write(price, new CD(title, artist, country, company, price, year));
    }
}
