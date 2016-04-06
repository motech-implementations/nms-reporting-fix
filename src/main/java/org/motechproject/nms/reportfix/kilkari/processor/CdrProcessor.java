package org.motechproject.nms.reportfix.kilkari.processor;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.kilkari.cache.LookupCache;
import org.motechproject.nms.reportfix.kilkari.domain.CdrRow;
import org.motechproject.nms.reportfix.kilkari.domain.SubscriptionInfo;
import org.motechproject.nms.reportfix.kilkari.helpers.Parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Processor to read cdr files and store them
 */
public class CdrProcessor {

    private LookupCache lookupCache;
    private MysqlDataSource reporting;

    public CdrProcessor() {
    }

    public void startProcessor(String directoryPath, LookupCache lookupCache, MysqlDataSource reporting) throws ParseException {
        if (lookupCache == null) {
            return;
        }

        this.lookupCache = lookupCache;
        this.reporting = reporting;
        File directory = new File(directoryPath);
        for (File currentFile : directory.listFiles()) {
            loadFile(currentFile);
        }
    }

    private void loadFile(File currentFile) throws ParseException {
        String fileName = currentFile.getName();
        DateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
        Date fileDate = format.parse(fileName.split("_")[3]);
        System.out.println(fileDate);

        ingestFile(currentFile);
    }

    private void ingestFile(File currentFile) {
        String currentLine;
        int saved = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(currentFile)); Connection repcon = this.reporting.getConnection()) {
            int lineCount = 0;

            while ((currentLine = br.readLine()) != null) {
                if (lineCount == 0) {
                    lineCount++;
                    continue;
                }

                CdrRow currentRow = new CdrRow();
                String[] properties = currentLine.split(",");
                currentRow.setSubscriptionId(properties[0].split(":")[1]);
                currentRow.setPhoneNumber(properties[1]);
                currentRow.setAttemptNumber(Parser.parseInt(properties[3]));
                currentRow.setStartTime(Parser.parseDate(properties[4]));
                currentRow.setAnswerTime(Parser.parseDate(properties[5]));
                currentRow.setEndTime(Parser.parseDate(properties[6]));
                currentRow.setCallDurationInPulses(Parser.parseInt(properties[7]));
                currentRow.setCallStatus(Parser.parseInt(properties[8]));
                currentRow.setLlId(Parser.parseInt(properties[9]));
                currentRow.setContentFilename(properties[10]);
                currentRow.setMsgStartTime(Parser.parseDate(properties[11]));
                currentRow.setMsgEndTime(Parser.parseDate(properties[12]));
                currentRow.setCircle(properties[13]);
                currentRow.setOperator(properties[14]);
                currentRow.setWeekId(properties[17]);

                if (saveRow(currentRow, repcon)) {
                    saved++;
                }
                lineCount++;
            }
            System.out.println("Read " + lineCount + " lines from file: " + currentFile.getName());
            System.out.println("Saved " + saved + " call detail records");
        } catch (IOException|SQLException ex) {
            System.out.println(ex.toString());
        }
    }

    private boolean saveRow(CdrRow cdrRow, Connection repcon) {
        SubscriptionInfo si = lookupCache.getSubscriptionInfo(cdrRow.getSubscriptionId());
        if (si == null) {
            return false;
        }

        return true;
    }
}
