package org.motechproject.nms.reportfix.kilkari;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.config.ConfigReader;
import org.motechproject.nms.reportfix.kilkari.cache.LookupCache;
import org.motechproject.nms.reportfix.kilkari.constants.KilkariConstants;
import org.motechproject.nms.reportfix.kilkari.domain.CdrRow;
import org.motechproject.nms.reportfix.kilkari.helpers.Parser;
import org.motechproject.nms.reportfix.kilkari.processor.CdrProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
    private CdrProcessor cdrProcessor;
    private LookupCache lookupCache;

    public KilkariFixer(MysqlDataSource prod, MysqlDataSource reporting, ConfigReader configReader) {
        this.production = prod;
        this.reporting = reporting;
        this.configReader = configReader;
        this.cdrProcessor = new CdrProcessor();
        lookupCache = new LookupCache();
    }

    public void start() throws ParseException {
        System.out.println("Fixing Kilkari data");
        try (Connection prod = production.getConnection(); Connection report = reporting.getConnection()){
            // getCallCount(report, "October", 2015);
            lookupCache.initialize(reporting, production);
            cdrProcessor.startProcessor(configReader.getProperty("cdr.directory"), lookupCache);
        } catch (SQLException sqle) {
            System.out.println("Unable to connect to motech or reporting db: " + sqle.toString());
        }
    }

    private void getCallCount(Connection connection, String monthName, int year) {
        Statement statement = null;
        ResultSet rs = null;
        String query = String.format(KilkariConstants.getCallCountSql, monthName, year);

        try {
            statement = connection.createStatement();
            rs = statement.executeQuery(query);
            while(rs.next()) {
                System.out.println(monthName + ": " + rs.getLong("monthlyCount"));
            }
        } catch (SQLException sqle) {
            System.out.println("Cannot get results from db: " + sqle.toString());
        } // todo: close statement and resultset?
    }






}
