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

    public static final String getOperatorCacheSql = "SELECT operator_code, ID FROM operator_dimension";

    public static final String getMessageDurationCacheSql = "SELECT Campaign_ID, Message_Duration FROM campaign_dimension";

    public static final String getCampaignMessageCacheSql = "SELECT Campaign_ID, ID FROM campaign_dimension";

    public static final String getSubscriptionPackCacheSql = "SELECT Subscription_Pack, Subscription_Pack_ID FROM subscription_pack_dimension";

    public static final String getChannelCacheSql = "SELECT Channel, ID FROM channel_dimension;";

    public static final String getSubscriptionInfoSql = "select Subscription_ID, Subscriber_Pack_ID, Start_Date  from subscriptions where SubscriptionId = '%s'";

    /**
     * Query for data insert
     */
    public static final String insertCdrRowSql = "INSERT INTO subscriber_call_measure\n"+
            "(Subscription_ID, Operator_ID, Subscription_Pack_ID, Campaign_ID, \n"+
            "Start_Date_ID, End_Date_ID, Start_Time_ID, End_Time_ID, State_ID, \n"+
            "Call_Status, Duration, call_duration, Service_Option, Percentage_Listened, Call_Source, \n"+
            "Subscription_Status, Duration_In_Pulse, Call_Start_Time, Call_End_Time, \n"+
            "Attempt_Number, Subscription_Start_Date, msg_duration, modificationDate)\n"+
            "VALUES\n"+
            "(%d, %d, %d, %d," +
            " %d, %d, %d, %d, %s," +
            " '%s', %d, %d, %s, %d, '%s'," +
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
    public static final String deleteRecordsForDay = "DELETE FROM subscriber_call_measure WHERE Call_Source = 'OBD' AND Start_Date_ID = %d";

    /**
     * used to get missing subscription info from production db
     */
    public static final String fetchSubscription = "SELECT * FROM nms_subscriptions WHERE subscriptionId = '%s'";

    /**
     * This is used to backfill the missing subscription info from CDR & prod
     */
    public static final String fetchMissingSubscriptionInfo = "SELECT \n" +
            "\ts.id as Subscriber_ID,\n" +
            "\tIF (sp.type = 'PREGNANCY', m.name, c.name) as Name,\n" +
            "\tl.name as Language,\n" +
            "\tm.dateOfBirth as Date_Of_Birth,\n" +
            "\tIF (sp.type = 'PREGNANCY', m.state_id_OID, c.state_id_OID) as State_ID,\n" +
            "\tIF (sp.type = 'PREGNANCY', m.district_id_OID, c.district_id_OID) as District_ID,\n" +
            "\tIF (sp.type = 'PREGNANCY', m.taluka_id_OID, c.taluka_id_OID) as Taluka_ID,\n" +
            "\tIF (sp.type = 'PREGNANCY', m.village_id_OID, c.village_id_OID) as Village_ID,\n" +
            "\tIF (sp.type = 'PREGNANCY', m.healthBlock_id_OID, c.healthBlock_id_OID) as HBlock_ID,\n" +
            "\tIF (sp.type = 'PREGNANCY', m.healthFacility_id_OID, c.healthFacility_id_OID) as HFacility_ID,\n" +
            "\tIF (sp.type = 'PREGNANCY', m.healthSubFacility_id_OID, c.healthSubFacility_id_OID) as HSub_Facility_ID,\n" +
            "\ts.lastMenstrualPeriod as Lmp,\n" +
            "    ss.id as Subscription_ID,\n" +
            "    sp.type as PackType,\n" +
            "    ss.origin as Channel,\n" +
            "    null as Operator,\n" +
            "    ss.status as Subscription_Status,\n" +
            "    ss.startDate as Start_Date,\n" +
            "    null as Old_Subscription_ID,\n" +
            "    s.callingNumber as MS_ISDN,\n" +
            "    ss.creationDate as Creation_Time,\n" +
            "    null as Old_Start_Date,\n" +
            "    ss.activationDate as Activation_Date\n" +
            "from nms_subscriptions as ss\n" +
            "join nms_subscription_packs as sp on sp.id = ss.subscriptionPack_id_OID\n" +
            "join nms_subscribers as s on s.id = ss.subscriber_id_OID\n" +
            "left join nms_languages as l on l.id = s.language_id_OID\n" +
            "left join nms_mcts_mothers as m on m.id = s.mother_id_OID\n" +
            "left join nms_mcts_children as c on c.id = s.child_id_OID\n" +
            "where subscriptionId = '%s'";

    /**
     * Used to insert missing subscriber in the db. Note that we use insert ignore because of terrible ETL logic
     * combined with terrible bugs in how subscriber id is managed in reporting. All that we care about here is the
     * location info contained and nothing else probably matters...except when it does #fml
     */
    public static final String insertMissingSubscriber = "INSERT IGNORE INTO subscribers\n" +
            "(Subscriber_ID, Name, Language, Age_Of_Beneficiary, Date_Of_Birth, " +
            "State_ID, District_ID, Taluka_ID, Village_ID, HBlock_ID, HFacility_ID, HSub_Facility_ID, " +
            "Last_Modified_Time, Lmp)\n" +
            "VALUES\n" +
            "(%d, '%s', '%s', %s, '%s', " +
            "%d, %d, %d, %d, %d, %d, %d, " +
            "'%s', '%s');";

    /**
     * Used to insert missing subscriptions in the reporting db.
     * In an ideal world wih intelligent life, we wouldn't use ignore in the insert but we got swindled.
     * Fool me once, can't get fooled again!
     */
    public static final String insertMissingSubscription = "INSERT IGNORE INTO subscriptions\n" +
            "(Subscription_ID, Subscriber_ID, Subscriber_Pack_ID, Channel_ID, Operator_ID, " +
            "Last_Modified_Time, Subscription_Status, Start_Date, Old_Subscription_ID, MS_ISDN, SubscriptionId, " +
            "Creation_Time, Old_Start_Date, Activation_Date)\n" +
            "VALUES\n" +
            "(%d, %d, %d, %d, %d, " +
            "'%s', '%s', '%s', '%s', %d, '%s', " +
            "'%s', %s, '%s')";
}
