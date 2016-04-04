package org.motechproject.nms.reportfix.kilkari;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.config.ConfigReader;

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
                currentRow.setStartTime(new Date(Long.parseLong(properties[4]) * 1000));
                currentRow.setAnswerTime(new Date(Long.parseLong(properties[5]) * 1000));
                currentRow.setEndTime(new Date(Long.parseLong(properties[6]) * 1000));
                currentRow.setCallDurationInPulses(Integer.parseInt(properties[7]));
                currentRow.setCallStatus(Integer.parseInt(properties[8]));
                currentRow.setLlId(Integer.parseInt(properties[9]));
                currentRow.setContentFilename(properties[10]);
                currentRow.setMsgStartTime(new Date(Long.parseLong(properties[11]) * 1000));
                currentRow.setMsgEndTime(new Date(Long.parseLong(properties[12]) * 1000));
                currentRow.setCircle(properties[13]);
                currentRow.setOperator(properties[14]);
                currentRow.setCallDisconnectReason(Integer.parseInt(properties[16]));
                currentRow.setWeekId(properties[17]);
            }

        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }
    }

}
