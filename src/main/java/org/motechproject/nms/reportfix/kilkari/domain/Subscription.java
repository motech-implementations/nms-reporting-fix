package org.motechproject.nms.reportfix.kilkari.domain;

import java.sql.Timestamp;

/**
 * Representation of subscription from the reporting perspective
 * ps. casing here matches the db schema
 */
public class Subscription {

    private Long Subscriber_ID;

    private int Subscriber_Pack_ID;

    private int Channel_ID;

    private int Operator_ID;

    private Timestamp Last_Modified_Time;

    private String Subscription_Status;

    private Timestamp Start_Date;

    private Long Old_Subscription_ID;

    private Long MS_ISDN;

    private String SubscriptionId;

    private Timestamp Creation_Time;

    private Timestamp Old_Start_Date;

    private Timestamp Activation_Date;

    public Long getSubscriber_ID() {
        return Subscriber_ID;
    }

    public void setSubscriber_ID(Long subscriber_ID) {
        Subscriber_ID = subscriber_ID;
    }

    public int getSubscriber_Pack_ID() {
        return Subscriber_Pack_ID;
    }

    public void setSubscriber_Pack_ID(int subscriber_Pack_ID) {
        Subscriber_Pack_ID = subscriber_Pack_ID;
    }

    public int getChannel_ID() {
        return Channel_ID;
    }

    public void setChannel_ID(int channel_ID) {
        Channel_ID = channel_ID;
    }

    public int getOperator_ID() {
        return Operator_ID;
    }

    public void setOperator_ID(int operator_ID) {
        Operator_ID = operator_ID;
    }

    public Timestamp getLast_Modified_Time() {
        return Last_Modified_Time;
    }

    public void setLast_Modified_Time(Timestamp last_Modified_Time) {
        Last_Modified_Time = last_Modified_Time;
    }

    public String getSubscription_Status() {
        return Subscription_Status;
    }

    public void setSubscription_Status(String subscription_Status) {
        Subscription_Status = subscription_Status;
    }

    public Timestamp getStart_Date() {
        return Start_Date;
    }

    public void setStart_Date(Timestamp start_Date) {
        Start_Date = start_Date;
    }

    public Long getOld_Subscription_ID() {
        return Old_Subscription_ID;
    }

    public void setOld_Subscription_ID(Long old_Subscription_ID) {
        Old_Subscription_ID = old_Subscription_ID;
    }

    public Long getMS_ISDN() {
        return MS_ISDN;
    }

    public void setMS_ISDN(Long MS_ISDN) {
        this.MS_ISDN = MS_ISDN;
    }

    public String getSubscriptionId() {
        return SubscriptionId;
    }

    public void setSubscriptionId(String subscriptionId) {
        SubscriptionId = subscriptionId;
    }

    public Timestamp getCreation_Time() {
        return Creation_Time;
    }

    public void setCreation_Time(Timestamp creation_Time) {
        Creation_Time = creation_Time;
    }

    public Timestamp getOld_Start_Date() {
        return Old_Start_Date;
    }

    public void setOld_Start_Date(Timestamp old_Start_Date) {
        Old_Start_Date = old_Start_Date;
    }

    public Timestamp getActivation_Date() {
        return Activation_Date;
    }

    public void setActivation_Date(Timestamp activation_Date) {
        Activation_Date = activation_Date;
    }
}
