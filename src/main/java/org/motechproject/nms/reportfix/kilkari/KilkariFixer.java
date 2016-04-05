package org.motechproject.nms.reportfix.kilkari;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.config.ConfigReader;
import org.motechproject.nms.reportfix.kilkari.cache.LookupCache;
import org.motechproject.nms.reportfix.kilkari.constants.KilkariConstants;
import org.motechproject.nms.reportfix.kilkari.domain.CdrRow;
import org.motechproject.nms.reportfix.kilkari.helpers.Parser;

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
    private LookupCache lookupCache;



    public KilkariFixer(MysqlDataSource prod, MysqlDataSource reporting, ConfigReader configReader) {
        this.production = prod;
        this.reporting = reporting;
        this.configReader = configReader;
        lookupCache = new LookupCache();
    }

    public void start() throws ParseException {
        System.out.println("Fixing Kilkari data");
        try (Connection prod = production.getConnection(); Connection report = reporting.getConnection()){
            // getCallCount(report, "October", 2015);
            lookupCache.initialize(reporting, production);
            File directory = new File(configReader.getProperty("cdr.directory"));
            for (File currentFile : directory.listFiles()) {
                ingestFile(currentFile);
            }
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

    private void ingestFile(File currentFile) throws ParseException {
        String fileName = currentFile.getName();
        DateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
        Date fileDate = format.parse(fileName.split("_")[3]);
        System.out.println(fileDate);

        loadFile(currentFile);
    }

    private void loadFile(File currentFile) {
        String currentLine;

        try (BufferedReader br = new BufferedReader(new FileReader(currentFile))) {
            int lineCount = 0;

            while ((currentLine = br.readLine()) != null) {
                if (lineCount == 0) {
                    lineCount++;
                    continue;
                }

                CdrRow currentRow = new CdrRow();
                String[] properties = currentLine.split(",");
                currentRow.setSubscriptionId(properties[0].split(":")[1]);
                currentRow.setPhoneNumber(properties[1]);
                currentRow.setAttemptNumber(Parser.parseInt(properties[3]));
                currentRow.setStartTime(Parser.parseDate(properties[4]));
                currentRow.setAnswerTime(Parser.parseDate(properties[5]));
                currentRow.setEndTime(Parser.parseDate(properties[6]));
                currentRow.setCallDurationInPulses(Parser.parseInt(properties[7]));
                currentRow.setCallStatus(Parser.parseInt(properties[8]));
                currentRow.setLlId(Parser.parseInt(properties[9]));
                currentRow.setContentFilename(properties[10]);
                currentRow.setMsgStartTime(Parser.parseDate(properties[11]));
                currentRow.setMsgEndTime(Parser.parseDate(properties[12]));
                currentRow.setCircle(properties[13]);
                currentRow.setOperator(properties[14]);
                currentRow.setWeekId(properties[17]);
            }
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }
    }



}
