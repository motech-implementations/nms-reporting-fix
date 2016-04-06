package org.motechproject.nms.reportfix.kilkari.domain;

import java.sql.Timestamp;

/**
 * Created by kosh on 4/6/16.
 */
public class SubscriptionInfo {

    private long Id;

    private int packId;

    private Timestamp startDate;

    public long getId() {
        return Id;
    }

    public void setId(long id) {
        Id = id;
    }

    public int getPackId() {
        return packId;
    }

    public void setPackId(int packId) {
        this.packId = packId;
    }

    public Timestamp getStartDate() {
        return startDate;
    }

    public void setStartDate(Timestamp startDate) {
        this.startDate = startDate;
    }
}
