package org.motechproject.nms.reportfix.kilkari.cache;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.kilkari.constants.KilkariConstants;
import org.motechproject.nms.reportfix.kilkari.domain.SubscriptionInfo;
import org.motechproject.nms.reportfix.logger.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private Map<String, Integer> subscriptionPackCache;
    private Map<String, Integer> channelCache;
    private Map<Integer, String> callStatusMap;
    private Map<String, SubscriptionInfo> subscriptionInfoCache;
    private HashSet<String> missingCache;

    private int missing;
    private int found;

    /**
     * Constructor
     */
    public LookupCache() {
        dateCache = new HashMap<>();
        timeCache = new HashMap<>();
        campaignMessageCache = new HashMap<>();
        messageDurationCache = new HashMap<>();
        operatorCache = new HashMap<>();
        subscriptionPackCache = new HashMap<>();
        channelCache = new HashMap<>();
        callStatusMap = new HashMap<>();
        subscriptionInfoCache = new HashMap<>();
        missingCache = new HashSet<>();
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
        initializeSubscriptionPackCache();
        initializeChannelCache();
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

        Logger.log("Call status map filled: " + callStatusMap.size() + " items");
    }

    private void initializeDateCache() {
        try (Connection connection = this.reporting.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(KilkariConstants.getDateCacheSql);
            while (rs.next()) {
                dateCache.put(rs.getString("FullDate"), rs.getInt("ID"));
            }

        } catch (SQLException sqle) {
            Logger.log("Cannot get date cache from reporting: " + sqle.toString());
        }

        Logger.log("Date cache filled: " + dateCache.size() + " items");
    }

    private void initializeTimeCache() {
        try (Connection connection = this.reporting.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(KilkariConstants.getTimeCacheSql);
            while (rs.next()) {
                timeCache.put(rs.getString("FullTime"), rs.getInt("ID"));
            }

        } catch (SQLException sqle) {
            Logger.log("Cannot get time cache from reporting: " + sqle.toString());
        }

        Logger.log("Time cache filled: " + timeCache.size() + " items");
    }

    private void initializeOperatorCache() {
        try (Connection connection = this.reporting.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(KilkariConstants.getOperatorCacheSql);
            while (rs.next()) {
                operatorCache.put(rs.getString("operator_code"), rs.getInt("ID"));
            }

        } catch (SQLException sqle) {
            Logger.log("Cannot get operator cache from reporting: " + sqle.toString());
        }

        Logger.log("Operator cache filled: " + operatorCache.size() + " items");
    }

    private void initializeMessageDurationCache() {
        try (Connection connection = this.reporting.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(KilkariConstants.getMessageDurationCacheSql);
            while (rs.next()) {
                messageDurationCache.put(rs.getString("Campaign_ID"), rs.getInt("Message_Duration"));
            }

        } catch (SQLException sqle) {
            Logger.log("Cannot get message duration cache from reporting: " + sqle.toString());
        }

        Logger.log("Message duration cache filled: " + messageDurationCache.size() + " items");
    }

    private void initializeCampaignMessageCache() {
        try (Connection connection = this.reporting.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(KilkariConstants.getCampaignMessageCacheSql);
            while (rs.next()) {
                campaignMessageCache.put(rs.getString("Campaign_ID"), rs.getInt("ID"));
            }

        } catch (SQLException sqle) {
            Logger.log("Cannot get message duration cache from reporting: " + sqle.toString());
        }

        Logger.log("Campaign message cache filled: " + campaignMessageCache.size() + " items");

    }

    private void initializeSubscriptionPackCache() {
        try (Connection connection = this.reporting.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(KilkariConstants.getSubscriptionPackCacheSql);
            while (rs.next()) {
                subscriptionPackCache.put(rs.getString("Subscription_Pack"), rs.getInt("Subscription_Pack_ID"));
            }

        } catch (SQLException sqle) {
            Logger.log("Cannot get subscription pack cache from reporting: " + sqle.toString());
        }

        Logger.log("Subscription pack cache filled: " + subscriptionPackCache.size() + " items");
    }

    private void initializeChannelCache() {
        try (Connection connection = this.reporting.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(KilkariConstants.getChannelCacheSql);
            while (rs.next()) {
                channelCache.put(rs.getString("Channel"), rs.getInt("ID"));
            }

        } catch (SQLException sqle) {
            Logger.log("Cannot get Channel cache from reporting: " + sqle.toString());
        }

        Logger.log("Channel cache filled: " + channelCache.size() + " items");
    }

    public String getCallStatus(int status) {
        return callStatusMap.get(status);
    }

    public int getDateId(String date) {
        return dateCache.get(date);
    }

    public int getTimeId(String time) {
        return timeCache.get(time);
    }

    public int getOperatorId(String operator) {
        return operatorCache.get(operator);
    }

    public int getMessageDuration(String message) {
        return messageDurationCache.get(message);
    }

    public int getCampaignId(String weekId) {
        return campaignMessageCache.get(weekId);
    }

    public int getSubscriptionPackId(String packType) {
        return subscriptionPackCache.get(packType);
    }

    public int getChannelId(String channel) {
        return channelCache.get(channel);
    }

    public SubscriptionInfo getSubscriptionInfo (String subscriptionId) {
        if (!subscriptionInfoCache.containsKey(subscriptionId)) {
            fetchAndSaveSubscriptionInfo(subscriptionId);
        }
        return subscriptionInfoCache.get(subscriptionId);
    }

    public void printStats() {
        Logger.log("***Lookup cache stats***");
        Logger.log("Subscription info cache size: " + subscriptionInfoCache.size());
        Logger.log("Missing subscription count: " + missingCache.size());

        Logger.log("Printing missing subscriptions -");
        for (String current : missingCache) {
            Logger.log(current);
        }
    }

    private void fetchAndSaveSubscriptionInfo(String subscriptionId) {
        SubscriptionInfo si = null;
        try (Connection connection = this.reporting.getConnection();
             Statement statement = connection.createStatement()) {

            String query = String.format(KilkariConstants.getSubscriptionInfoSql, subscriptionId);
            ResultSet rs = statement.executeQuery(query);

            while (rs.next()) {
                si = new SubscriptionInfo();
                si.setId(rs.getLong("Subscription_ID"));
                si.setPackId(rs.getInt("Subscriber_Pack_ID"));
                si.setStartDate(rs.getTimestamp("Start_Date"));
            }
            if (si == null) {
                missing++;
                missingCache.add(subscriptionId);
            } else {
                found++;
            }
            if (subscriptionInfoCache.size() % 10000 == 0) {
                Logger.log(String.format("Subscription cache: %d Found: %d Missing: %d", subscriptionInfoCache.size(), found, missing));
            }
            subscriptionInfoCache.put(subscriptionId, si);
        } catch (SQLException sqle) {
            Logger.log("Cannot get subscriptionInfo: " + sqle.toString());
        }
    }
}
