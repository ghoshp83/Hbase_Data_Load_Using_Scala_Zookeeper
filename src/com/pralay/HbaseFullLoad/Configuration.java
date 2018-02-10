package com.pralay.HbaseFullLoad;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Configuration implements Serializable {

    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    private final HashMap<String, String> attr;
    private static Configuration instance = null;
    private static String configPath;
    public static Configuration getInstance() {

        if (instance == null) {
            instance = new Configuration(configPath);
        }
        return instance;
    }

    public static void init(String configPath) {
        Configuration.configPath = configPath;
    }

    public String getValue(final String key, final String defaultValue) {
        String result = attr.get(key);
        LOGGER.debug("Read from XML: " + key + " -> " + result);
        if (result == null || Objects.equals(result, "")) {
            return defaultValue;
        }
        return result;
    }

    public String getValue(final String key) {
        LOGGER.debug("Read from XML: " + key + " -> " + attr.get(key));
        return attr.get(key);
    }

    private Configuration(String configPath) {
        attr = new HashMap<String, String>();
        read(configPath);
    }

    private boolean read(String configPath) {
        final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        Path path = new Path(configPath);

        FileSystem fs = null;
        FSDataInputStream inputStream = null;
        try {
            fs = FileSystem.get(path.toUri(), conf);
            inputStream = fs.open(path);
        } catch (IOException e) {
            LOGGER.error("Config file cannot be opened:"+e.getMessage());
            throw new FatalException(e);

        }

        DocumentBuilder dBuilder;
        try {
            dBuilder = dbFactory.newDocumentBuilder();
        } catch (final ParserConfigurationException e) {
            LOGGER.error("Config file cannot be parsed:"+e.getMessage());
            throw new FatalException(e);
        }

        Document doc;
        try {
            doc = dBuilder.parse(inputStream);
        } catch (Exception e) {
            LOGGER.error("Config file cannot be parsed:"+e.getMessage());
            throw new FatalException(e);
        }

        doc.getDocumentElement().normalize();

        final NodeList nList = doc.getElementsByTagName("property");
        for (int temp = 0; temp < nList.getLength(); temp++) {
            final Node nNode = nList.item(temp);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                final Element eElement = (Element) nNode;
                final String name = eElement.getElementsByTagName("name").item(0).getTextContent();
                final NodeList valueNodeList = eElement.getElementsByTagName("value");
                for (int i = 0; i < valueNodeList.getLength(); i++) {
                    final String value = eElement.getElementsByTagName("value").item(i).getTextContent();
                    final Element attributeElement = (Element) eElement.getElementsByTagName("value").item(i);
                    final String attribute = attributeElement.getAttribute("input");
                    attr.put(name + attribute, value);
                }
            }
        }

        try {
            fs.close();
        } catch (IOException e) {
            LOGGER.error("Config file cannot be closed:"+e.getMessage());
            throw new FatalException(e);
        }

        return true;
    }
}
