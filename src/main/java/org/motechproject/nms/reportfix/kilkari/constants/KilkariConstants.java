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

}
