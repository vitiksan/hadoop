package com.learning;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;

public class XMLHandler extends DefaultHandler {
    private boolean isTitle = false;
    private boolean isArtist = false;
    private boolean isCountry = false;
    private boolean isCompany = false;
    private boolean isPrice = false;
    private boolean isYear = false;


    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

        if (qName.equalsIgnoreCase("cd"))
            System.out.println("****************************\nCD:");
        else if (qName.equalsIgnoreCase("artist"))
            isArtist = true;
        else if (qName.equalsIgnoreCase("country"))
            isCountry = true;
        else if (qName.equalsIgnoreCase("company"))
            isCompany = true;
        else if (qName.equalsIgnoreCase("price"))
            isPrice = true;
        else if (qName.equalsIgnoreCase("year"))
            isYear = true;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (qName.equalsIgnoreCase("cd"))
            System.out.println("****************************");
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (isTitle) {
            System.out.println("Title: " + new String(ch, start, length));
            isTitle = false;
        } else if (isArtist) {
            System.out.println("Artist: " + new String(ch, start, length));
            isArtist = false;
        } else if (isCountry) {
            System.out.println("Country: " + new String(ch, start, length));
            isCountry = false;
        } else if (isCompany) {
            System.out.println("Company: " + new String(ch, start, length));
            isCompany = false;
        } else if (isPrice) {
            System.out.println("Price: " + new String(ch, start, length));
            isPrice = false;
        } else if (isYear) {
            System.out.println("Year: " + new String(ch, start, length));
            isYear = false;
        }
    }

    public static void main(String[] args) {
        try {
            File input = new File("src/main/resources/input.xml");
            SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            XMLHandler handler = new XMLHandler();
            parser.parse(input, handler);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
