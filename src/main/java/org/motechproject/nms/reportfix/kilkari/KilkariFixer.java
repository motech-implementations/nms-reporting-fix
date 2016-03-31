package org.motechproject.nms.reportfix.kilkari;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.config.ConfigReader;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class to handle and fix/inject kilkari CDR call data from files
 */
public class KilkariFixer {

    private MysqlDataSource production;
    private MysqlDataSource reporting;
    private ConfigReader configReader;

    public KilkariFixer(MysqlDataSource prod, MysqlDataSource reporting, ConfigReader configReader) {
        this.production = prod;
        this.reporting = reporting;
        this.configReader = configReader;
    }

    public void start() throws ParseException {
        Connection prod = null;
        Connection report = null;
        System.out.println("Fixing Kilkari data");
        try {
            prod = production.getConnection();
            report = reporting.getConnection();
            getCallCount(report, "October", 2015);
            File directory = new File(configReader.getProperty("cdr.directory"));
            for (File currentFile : directory.listFiles()) {
                ingestFile(currentFile);
            }
        } catch (SQLException sqle) {
            System.out.println("Unable to connect to motech or reporting db: " + sqle.toString());
            return;
        } // todo: close connection?
    }

    private void getCallCount(Connection connection, String monthName, int year) {
        Statement statement = null;
        ResultSet rs = null;
        String query = String.format("SELECT count(*) as monthlyCount FROM date_dimension as dd " +
                "join subscriber_call_measure as scm on scm.Start_Date_ID = dd.ID " +
                "where DimMonth = '%s' and DimYear = '%d'", monthName, year);

        try {
            statement = connection.createStatement();
            rs = statement.executeQuery(query);
            while(rs.next()) {
                System.out.println(monthName + ": " + rs.getLong("monthlyCount"));
            }
        } catch (SQLException sqle) {
            System.out.println("Cannot get results from db: " + sqle.toString());
        } // todo: close shit
    }

    private void ingestFile(File currentFile) throws ParseException {
        String fileName = currentFile.getName();
        DateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
        Date fileDate = format.parse(fileName.split("_")[3]);
        System.out.println(fileDate);
    }
}
