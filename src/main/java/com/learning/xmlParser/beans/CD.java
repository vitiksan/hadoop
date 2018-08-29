package com.learning.xmlParser.beans;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CD implements WritableComparable<CD> {
    private Text title = new Text();
    private Text artist = new Text();
    private Text country = new Text();
    private Text company = new Text();
    private DoubleWritable price = new DoubleWritable();
    private IntWritable year = new IntWritable();

    public CD() {
    }

    public CD(Text title, Text artist, Text country, Text company,
              DoubleWritable price, IntWritable year) {
        this.title = title;
        this.artist = artist;
        this.country = country;
        this.company = company;
        this.price = price;
        this.year = year;
    }

    public Text getTitle() {
        return title;
    }

    public void setTitle(Text title) {
        this.title = title;
    }

    public Text getArtist() {
        return artist;
    }

    public void setArtist(Text artist) {
        this.artist = artist;
    }

    public Text getCountry() {
        return country;
    }

    public void setCountry(Text country) {
        this.country = country;
    }

    public Text getCompany() {
        return company;
    }

    public void setCompany(Text company) {
        this.company = company;
    }

    public DoubleWritable getPrice() {
        return price;
    }

    public void setPrice(DoubleWritable price) {
        this.price = price;
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        title.write(dataOutput);
        artist.write(dataOutput);
        country.write(dataOutput);
        company.write(dataOutput);
        price.write(dataOutput);
        year.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        title.readFields(dataInput);
        artist.readFields(dataInput);
        country.readFields(dataInput);
        company.readFields(dataInput);
        price.readFields(dataInput);
        year.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "CD{" +
                "title=" + title.toString() +
                ", artist=" + artist.toString() +
                ", country=" + country.toString() +
                ", company=" + company.toString() +
                ", price=" + price.toString() +
                ", year=" + year.toString() +
                '}';
    }

    @Override
    public int compareTo(CD o) {
        if (this.title.equals(o.title) && this.artist.equals(o.artist))
            return 0;
        else
            return (this.artist.compareTo(o.artist) != 0) ? this.artist.compareTo(o.artist) :
                    this.title.compareTo(o.title);
    }
}
