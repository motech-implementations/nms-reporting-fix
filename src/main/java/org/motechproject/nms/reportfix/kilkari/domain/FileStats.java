package org.motechproject.nms.reportfix.kilkari.domain;

import java.util.Date;

/**
 * File process stats
 */
public class FileStats {

    private String fileName;

    private int totalLines;

    private int totalSaved;

    private int totalDuplicates;

    private Date startTime;

    private Date endTime;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getTotalLines() {
        return totalLines;
    }

    public void setTotalLines(int totalLines) {
        this.totalLines = totalLines;
    }

    public int getTotalSaved() {
        return totalSaved;
    }

    public void setTotalSaved(int totalSaved) {
        this.totalSaved = totalSaved;
    }

    public int getTotalDuplicates() {
        return totalDuplicates;
    }

    public void setTotalDuplicates(int totalDuplicates) {
        this.totalDuplicates = totalDuplicates;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
}
