package org.motechproject.nms.reportfix;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.config.ConfigReader;
import org.motechproject.nms.reportfix.kilkari.KilkariFixer;

import java.text.ParseException;

/**
 * Main class to run fix nms reporting
 */
public class ReportFix {

    public static void main(String[] args) throws ParseException{
        System.out.println("Hello World");

        System.out.println("Loading properties");
        ConfigReader configReader = new ConfigReader();
        readConfigValues(configReader);
        System.out.println("Loaded config");


        KilkariFixer kilkariFixer = new KilkariFixer(setProdDataSource(configReader), setReportDataSource(configReader), configReader);
        kilkariFixer.start();
    }

    private static void readConfigValues(ConfigReader configReader) {
        System.out.println("CDR directory: " + configReader.getProperty("cdr.directory"));
        System.out.println("Prod db server: " + configReader.getProperty("prod.db.server"));
        System.out.println("Prod db name: " + configReader.getProperty("prod.db.name"));
        System.out.println("Prod db username: " + configReader.getProperty("prod.db.username"));
        System.out.println("Prod db password: " + configReader.getProperty("prod.db.password"));
        System.out.println("Report db server: " + configReader.getProperty("report.db.server"));
        System.out.println("Report db name: " + configReader.getProperty("report.db.name"));
        System.out.println("Report db username: " + configReader.getProperty("report.db.username"));
        System.out.println("Report db password: " + configReader.getProperty("report.db.password"));
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
