package org.motechproject.nms.reportfix.config;

import org.motechproject.nms.reportfix.logger.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Config helper to get properties
 */
public class ConfigReader {
    private static String configFileName = "config.properties";
    private InputStream inputStream;
    private Properties properties;

    public ConfigReader() {
        this(configFileName);
    }

    public ConfigReader(String configFileName) {
        this.properties = new Properties();
        inputStream = getClass().getClassLoader().getResourceAsStream(configFileName);
        try {
            if (inputStream != null) {
                this.properties.load(inputStream);
            } else {
                throw new FileNotFoundException("No property file with name " + configFileName + " found");
            }
        } catch (IOException ioe) {
            Logger.log("Unable to load config. Check file. " + ioe.toString());
        }
    }

    public String getProperty(String propertyName) {
        return this.properties.getProperty(propertyName);
    }
}
