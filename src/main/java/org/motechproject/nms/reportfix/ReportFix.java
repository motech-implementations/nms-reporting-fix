package org.motechproject.nms.reportfix;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.config.ConfigReader;
import org.motechproject.nms.reportfix.kilkari.KilkariFixer;
import org.motechproject.nms.reportfix.logger.Logger;

import java.text.ParseException;

/**
 * Main class to run fix nms reporting
 */
public class ReportFix {

    public static void main(String[] args) throws ParseException{
        Logger.log("Hello World");

        Logger.log("Loading properties");
        ConfigReader configReader = new ConfigReader();
        readConfigValues(configReader);
        Logger.log("Loaded config");

        KilkariFixer kilkariFixer = new KilkariFixer(setProdDataSource(configReader), setReportDataSource(configReader), configReader);
        kilkariFixer.start();
    }

    private static void readConfigValues(ConfigReader configReader) {
        Logger.log("CDR directory: " + configReader.getProperty("cdr.directory"));
        Logger.log("Prod db server: " + configReader.getProperty("prod.db.server"));
        Logger.log("Prod db name: " + configReader.getProperty("prod.db.name"));
        Logger.log("Prod db username: " + configReader.getProperty("prod.db.username"));
        Logger.log("Prod db password: " + configReader.getProperty("prod.db.password"));
        Logger.log("Report db server: " + configReader.getProperty("report.db.server"));
        Logger.log("Report db name: " + configReader.getProperty("report.db.name"));
        Logger.log("Report db username: " + configReader.getProperty("report.db.username"));
        Logger.log("Report db password: " + configReader.getProperty("report.db.password"));
    }

    private static MysqlDataSource setProdDataSource(ConfigReader configReader) {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUser(configReader.getProperty("prod.db.username"));
        dataSource.setPassword(configReader.getProperty("prod.db.password"));
        dataSource.setServerName(configReader.getProperty("prod.db.server"));
        dataSource.setDatabaseName(configReader.getProperty("prod.db.name"));
        return dataSource;
    }

    private static MysqlDataSource setReportDataSource(ConfigReader configReader) {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUser(configReader.getProperty("report.db.username"));
        dataSource.setPassword(configReader.getProperty("report.db.password"));
        dataSource.setServerName(configReader.getProperty("report.db.server"));
        dataSource.setDatabaseName(configReader.getProperty("report.db.name"));
        return dataSource;
    }
}
