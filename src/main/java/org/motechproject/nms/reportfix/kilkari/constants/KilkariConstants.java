package org.motechproject.nms.reportfix.kilkari.constants;

/**
 * Constants used by kilkari fixer
 */
public final class KilkariConstants {

    /**
     * Queries for kilkari reporting lookup
     */
    public static final String getCallCountSql = "SELECT count(*) as monthlyCount FROM date_dimension as dd " +
            "join subscriber_call_measure as scm on scm.Start_Date_ID = dd.ID " +
            "where DimMonth = '%s' and DimYear = '%d'";

    public static final String getTimeCacheSql = "SELECT ID, FullTime FROM time_dimension";

    public static final String getDateCacheSql = "SELECT ID, FullDate from date_dimension";

    public static final String getOperatorCacheSql = "SELECT operator_code, ID FROM ananya_kilkari.operator_dimension";

    public static final String getMessageDurationCacheSql = "SELECT Campaign_ID, Message_Duration FROM ananya_kilkari.campaign_dimension";

    public static final String getCampaignMessageCacheSql = "SELECT Campaign_ID, ID FROM ananya_kilkari.campaign_dimension";

    public static final String getSubscriptionInfoSql = "select Subscription_ID, Subscriber_Pack_ID, Start_Date  from subscriptions where SubscriptionId = '%s'";

    /**
     * Query for data insert
     */
    public static final String insertCdrRowSql = "INSERT INTO subscriber_call_measure\n"+
            "(Subscription_ID, Operator_ID, Subscription_Pack_ID, Campaign_ID, \n"+
            "Start_Date_ID, End_Date_ID, Start_Time_ID, End_Time_ID, State_ID, \n"+
            "Call_Status, Duration, Service_Option, Percentage_Listened, Call_Source, \n"+
            "Subscription_Status, Duration_In_Pulse, Call_Start_Time, Call_End_Time, \n"+
            "Attempt_Number, Subscription_Start_Date, msg_duration, modificationDate)\n"+
            "VALUES\n"+
            "(%d, %d, %d, %d," +
            " %d, %d, %d, %d, %s," +
            " '%s', %d, %s, %d, '%s'," +
            " '%s', %d, '%s', '%s'," +
            " %d, '%s', %d, '%s');";

    /**
     * Query to check if subscription with id exists in reporting already
     */
    public static final String getSubscriptionRow = "SELECT count(*) FROM subscriptions WHERE SubscriptionId = '%s'";

    /**
     * Toggle safe updates on the db (like delete without using primary key)     *
     * This is currently being used for deleting all call measures for a day while loading CDRs
     */
    public static final String setSafeUpdates = "SET SQL_SAFE_UPDATES = %d";

    /**
     * Used to clear out existing subscriber call records for the day when we process CDR files
     */
    public static final String deleteRecordsForDay = "DELETE FROM subscriber_call_measure WHERE Start_Date_ID = %d";
}
