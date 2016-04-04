package org.motechproject.nms.reportfix.kilkari;

import java.util.Date;

/**
 * Class to represent each row in a CDR file
 */
public class CdrRow {
    String subscriptionId;
    String phoneNumber;
    Date startTime;
    Date answerTime;
    Date endTime;
    int callDurationInPulses;
    int callStatus;
    int llId;
    String contentFilename;
    Date msgStartTime;
    Date msgEndTime;
    String circle;
    String operator;
    int callDisconnectReason;
    String weekId;

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getAnswerTime() {
        return answerTime;
    }

    public void setAnswerTime(Date answerTime) {
        this.answerTime = answerTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public int getCallDurationInPulses() {
        return callDurationInPulses;
    }

    public void setCallDurationInPulses(int callDurationInPulses) {
        this.callDurationInPulses = callDurationInPulses;
    }

    public int getCallStatus() {
        return callStatus;
    }

    public void setCallStatus(int callStatus) {
        this.callStatus = callStatus;
    }

    public int getLlId() {
        return llId;
    }

    public void setLlId(int llId) {
        this.llId = llId;
    }

    public String getContentFilename() {
        return contentFilename;
    }

    public void setContentFilename(String contentFilename) {
        this.contentFilename = contentFilename;
    }

    public Date getMsgStartTime() {
        return msgStartTime;
    }

    public void setMsgStartTime(Date msgStartTime) {
        this.msgStartTime = msgStartTime;
    }

    public Date getMsgEndTime() {
        return msgEndTime;
    }

    public void setMsgEndTime(Date msgEndTime) {
        this.msgEndTime = msgEndTime;
    }

    public String getCircle() {
        return circle;
    }

    public void setCircle(String circle) {
        this.circle = circle;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public int getCallDisconnectReason() {
        return callDisconnectReason;
    }

    public void setCallDisconnectReason(int callDisconnectReason) {
        this.callDisconnectReason = callDisconnectReason;
    }

    public String getWeekId() {
        return weekId;
    }

    public void setWeekId(String weekId) {
        this.weekId = weekId;
    }
}
