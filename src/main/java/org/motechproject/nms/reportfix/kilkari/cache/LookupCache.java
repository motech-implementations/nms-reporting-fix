package org.motechproject.nms.reportfix.kilkari.cache;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.kilkari.constants.KilkariConstants;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Lookup cache class to get date, time and other static IDs
 */
public class LookupCache {

    private MysqlDataSource reporting;
    private MysqlDataSource production;

    private Map<String, Integer> dateCache;
    private Map<String, Integer> timeCache;
    private Map<String, Integer> campaignMessageCache;
    private Map<String, Integer> messageDurationCache;
    private Map<String, Integer> operatorCache;
    private Map<Integer, String> callStatusMap;

    /**
     * Constructor
     */
    public LookupCache() {
        dateCache = new HashMap<>();
        timeCache = new HashMap<>();
        campaignMessageCache = new HashMap<>();
        messageDurationCache = new HashMap<>();
        operatorCache = new HashMap<>();
        callStatusMap = new HashMap<>();
    }

    /**
     * Initialize the cacheable objects for faster lookups
     * @param reporting reporting db source
     * @param production production db source
     */
    public void initialize(MysqlDataSource reporting, MysqlDataSource production) {
        this.reporting = reporting;
        this.production = production;
        initializeCallStatusMap();
        initializeDateCache();
        initializeTimeCache();
        initializeOperatorCache();
        initializeMessageDurationCache();
        initializeCampaignMessageCache();
    }

    private void initializeCallStatusMap() {
        this.callStatusMap.put(1001, "SUCCESS");
        this.callStatusMap.put(2000, "ND"); // OBD_FAILED_ATTEMPT
        this.callStatusMap.put(2001, "ND"); // OBD_FAILED_BUSY
        this.callStatusMap.put(2004, "ND"); // OBD_FAILED_INVALIDNUMBER
        this.callStatusMap.put(2005, "ND"); // OBD_FAILED_OTHERS
        this.callStatusMap.put(3001, "ND"); // OBD_DNIS_IN_DND
        this.callStatusMap.put(2002, "NA"); // OBD_FAILED_NOANSWER
        this.callStatusMap.put(2003, "SO"); // OBD_FAILED_SWITCHEDOFF

        System.out.println("Call status map filled: " + callStatusMap.size() + " items");
    }

    private void initializeDateCache() {
        try (Connection connection = this.reporting.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(KilkariConstants.getDateCacheSql);
            while (rs.next()) {
                dateCache.put(rs.getString("FullDate"), rs.getInt("ID"));
            }

        } catch (SQLException sqle) {
            System.out.println("Cannot get date cache from reporting: " + sqle.toString());
        }

        System.out.println("Date cache filled: " + dateCache.size() + " items");
    }

    private void initializeTimeCache() {
        try (Connection connection = this.reporting.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(KilkariConstants.getTimeCacheSql);
            while (rs.next()) {
                timeCache.put(rs.getString("FullTime"), rs.getInt("ID"));
            }

        } catch (SQLException sqle) {
            System.out.println("Cannot get time cache from reporting: " + sqle.toString());
        }

        System.out.println("Time cache filled: " + timeCache.size() + " items");
    }

    private void initializeOperatorCache() {
        try (Connection connection = this.reporting.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(KilkariConstants.getOperatorCacheSql);
            while (rs.next()) {
                operatorCache.put(rs.getString("operator_code"), rs.getInt("ID"));
            }

        } catch (SQLException sqle) {
            System.out.println("Cannot get operator cache from reporting: " + sqle.toString());
        }

        System.out.println("Operator cache filled: " + operatorCache.size() + " items");
    }

    private void initializeMessageDurationCache() {
        try (Connection connection = this.reporting.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(KilkariConstants.getMessageDurationCacheSql);
            while (rs.next()) {
                messageDurationCache.put(rs.getString("Campaign_ID"), rs.getInt("Message_Duration"));
            }

        } catch (SQLException sqle) {
            System.out.println("Cannot get message duration cache from reporting: " + sqle.toString());
        }

        System.out.println("Message duration cache filled: " + messageDurationCache.size() + " items");
    }

    private void initializeCampaignMessageCache() {
        try (Connection connection = this.reporting.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(KilkariConstants.getCampaignMessageCacheSql);
            while (rs.next()) {
                campaignMessageCache.put(rs.getString("Campaign_ID"), rs.getInt("ID"));
            }

        } catch (SQLException sqle) {
            System.out.println("Cannot get message duration cache from reporting: " + sqle.toString());
        }

        System.out.println("Campaign message cache filled: " + campaignMessageCache.size() + " items");

    }
}