package org.motechproject.nms.reportfix.kilkari.importer;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.kilkari.cache.LookupCache;
import org.motechproject.nms.reportfix.kilkari.constants.KilkariConstants;
import org.motechproject.nms.reportfix.logger.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Class to import missing subscriptions from production into reporting
 */
public class SubscriptionImporter {

    private MysqlDataSource reporting;
    private MysqlDataSource production;
    private String directoryPath;
    private HashSet<String> missingSubscriptionSet;
    private LookupCache lookupCache;

    private int missing;
    private int found;

    public SubscriptionImporter() {
        this.missingSubscriptionSet = new HashSet<>();
        missing = 0;
        found = 0;
    }

    public void startImport(String directoryPath, MysqlDataSource production, MysqlDataSource reporting, LookupCache lookupCache) {

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+5:30"));
        this.directoryPath = directoryPath;
        this.production = production;
        this.reporting = reporting;
        this.lookupCache = lookupCache;
        findMissingSubscriptions();
        fetchMissingSubscriptions();
    }

    private void fetchMissingSubscriptions() {
        int missing  = missingSubscriptionSet.size();
        int found = 0;
        int subscribersAdded = 0;
        int subscriptionsAdded = 0;
        try (Connection prodcon = this.production.getConnection(); Connection repcon = this.reporting.getConnection()) {
            Statement statement = prodcon.createStatement();
            String query = null;
            for (String current : missingSubscriptionSet) {
                query = String.format(KilkariConstants.fetchMissingSubscriptionInfo, current);
                ResultSet rs = statement.executeQuery(query);
                if (rs.next()) {
                    found++;
                    if (addMissingSubscriber(repcon, rs)) {
                        subscribersAdded++;
                    }
                    if (addMissingSubscription(repcon, rs, current)) {
                        subscriptionsAdded++;
                    }
                }
            }
        } catch (SQLException sqle) {
            Logger.log("Unable to fetch subscriptions from production: " + sqle);
        }

        Logger.log("***Subscription recovery summary***");
        Logger.log("Missing: " + missing);
        Logger.log("Found: " + found);
        Logger.log("Subscribers added: " + subscribersAdded);
        Logger.log("Subscriptions added: " + subscriptionsAdded);
    }

    private boolean addMissingSubscription(Connection connection, ResultSet rs, String subscriptionId) {
        try {
            Long Subscription_ID = rs.getLong("Subscription_ID");
            Long Subscriber_ID = rs.getLong("Subscriber_ID");
            int Subscriber_Pack_ID = lookupCache.getSubscriptionPackId(rs.getString("PackType"));
            int Channel_ID = lookupCache.getChannelId(rs.getString("Channel"));
            Integer Operator_ID = null;
            Timestamp Last_Modified_Time = getModificationTime();
            Statement statement = connection.createStatement();
            String Subscription_Status = getSubscriptionStatus(rs.getString("Subscription_Status"));
            Timestamp Start_Date = rs.getTimestamp("Start_Date");
            Integer Old_Subscription_ID = null;
            Long MS_ISDN = rs.getLong("MS_ISDN");
            Timestamp Creation_Time = rs.getTimestamp("Creation_Time");
            Timestamp Old_Start_Date = null;
            Timestamp ActivationDate = rs.getTimestamp("Activation_Date");

            String query = String.format(KilkariConstants.insertMissingSubscription,
                    Subscription_ID, Subscriber_ID, Subscriber_Pack_ID, Channel_ID, Operator_ID, Last_Modified_Time,
                    Subscription_Status, Start_Date, Old_Subscription_ID, MS_ISDN, subscriptionId, Creation_Time,
                    Old_Start_Date, ActivationDate);
            statement.executeUpdate(query);
        } catch (SQLException sqle) {
            Logger.log("Couldn't add missing subscription: " + sqle.toString());
            return false;
        }
        return true;
    }

    private boolean addMissingSubscriber(Connection connection, ResultSet rs) {
        try {
            Long Subscriber_ID = rs.getLong("Subscriber_ID");
            String Name = rs.getString("Name");
            String Language = rs.getString("Language");
            Integer Age_Of_Beneficiary = null;
            Date Date_Of_Birth = rs.getDate("Date_Of_Birth");
            Integer State_ID = rs.getInt("State_ID");
            Integer District_ID = rs.getInt("District_ID");
            Integer Taluka_ID = rs.getInt("Taluka_ID");
            Integer Village_ID = rs.getInt("Village_ID");
            Integer HBlock_ID = rs.getInt("HBlock_ID");
            Integer HFacility_ID = rs.getInt("HFacility_ID");
            Integer HSub_Facility_ID = rs.getInt("HSub_Facility_ID");
            Timestamp Last_Modified_Time = getModificationTime();
            Date Lmp = rs.getDate("Lmp");

            Statement statement = connection.createStatement();
            String query = String.format(KilkariConstants.insertMissingSubscriber,
                    Subscriber_ID, Name, Language, Age_Of_Beneficiary, Date_Of_Birth,
                    State_ID, District_ID, Taluka_ID, Village_ID, HBlock_ID,
                    HFacility_ID, HSub_Facility_ID, Last_Modified_Time, Lmp);
            statement.executeUpdate(query);
        } catch (SQLException sqle) {
            Logger.log("Couldn't add missing subscriber: " + sqle.toString());
            return false;
        }
        return true;
    }

    private void findMissingSubscriptions() {
        File directory = new File(this.directoryPath);
        List<File> files = Arrays.asList(directory.listFiles());
        Collections.sort(files);
        Logger.log(String.format("Found %d files", files.size()));

        int index = 1;
        for (File currentFile : files) {
            Logger.log(String.format("Loading file %d of %d", index, files.size()));
            try {
                loadFile(currentFile);
            } catch (ParseException pex) {
                Logger.log("Unable to read file: " + currentFile.getName());
            }

            index += 1;
            Logger.log("Current missing subscription count: " + missingSubscriptionSet.size());
        }

        Logger.log("Final missing subscription count: " + missingSubscriptionSet.size());
        
    }

    private void loadFile(File currentFile) throws ParseException {
        String fileName = currentFile.getName();
        DateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
        Date fileDate = format.parse(fileName.split("_")[3]);
        Logger.log(fileDate.toString());
        String currentLine;

        try (BufferedReader br = new BufferedReader(new FileReader(currentFile)); Connection repcon = this.reporting.getConnection()) {

            int lineCount = 0;

            while ((currentLine = br.readLine()) != null) {
                if (lineCount == 0) {
                    lineCount++;
                    continue;
                }

                if (lineCount % 100000 == 0) {
                    Logger.log(String.format("Read %d lines", lineCount));
                }

                String subscriptionId = currentLine.split(",")[0].trim().split(":")[1].trim();
                Statement statement = repcon.createStatement();
                String query = String.format(KilkariConstants.getSubscriptionRow, subscriptionId);
                ResultSet rs = statement.executeQuery(query);
                rs.next();
                if (rs.getInt(1) == 0) {
                    missingSubscriptionSet.add(subscriptionId);
                }
                lineCount++;
            }
        } catch (SQLException|IOException ex) {
            Logger.log(ex.toString());
        }
    }

    // Get static modification time. Useful for debugging/undoing things in the future
    private Timestamp getModificationTime() {
        Date date = new Date();
        try {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            date = dateFormat.parse("2016-04-18 18:04:16");
        } catch (ParseException pex) {
            Logger.log("Unable to parse time, using current time: " + pex.toString());
        }

        return new Timestamp(date.getTime());
    }

    private String getSubscriptionStatus(String status) {
        if (status.compareTo("DEACTIVATED") == 0 || status.compareTo("DEACTIVATED_BY_USER") == 0) {
            return status;
        } else {
            return "COMPLETED";
        }
    }

}
