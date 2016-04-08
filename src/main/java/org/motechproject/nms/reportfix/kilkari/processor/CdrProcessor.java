package org.motechproject.nms.reportfix.kilkari.processor;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.kilkari.cache.LookupCache;
import org.motechproject.nms.reportfix.kilkari.constants.KilkariConstants;
import org.motechproject.nms.reportfix.kilkari.domain.CdrRow;
import org.motechproject.nms.reportfix.kilkari.domain.SubscriptionInfo;
import org.motechproject.nms.reportfix.kilkari.helpers.Parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Processor to read cdr files and store them
 */
public class CdrProcessor {

    private LookupCache lookupCache;
    private MysqlDataSource reporting;

    private static int totalLines;
    private static int totalDuplicates;
    private static int totalSaved;

    public CdrProcessor() {
    }

    public void startProcessor(String directoryPath, LookupCache lookupCache, MysqlDataSource reporting) throws ParseException {
        if (lookupCache == null) {
            return;
        }

        Date startTime = new Date();
        this.lookupCache = lookupCache;
        this.reporting = reporting;
        File directory = new File(directoryPath);
        List<File> files = Arrays.asList(directory.listFiles());
        Collections.sort(files);
        System.out.println(String.format("Found %d files", files.size()));
        parallelLoadFiles(files);
        /* int index = 1;
        for (File currentFile : files) {
            System.out.println(String.format("Loading file %d of %d", index, files.size()));
            loadFile(currentFile);
            index += 1;
        }
        */
        Date endTime = new Date();
        System.out.println("Start: " + startTime.toString() + " End: " + endTime.toString());
        System.out.println(String.format("%s processed. Total records: %d, Saved: %d, Duplicates: %d", directoryPath, totalLines, totalSaved, totalDuplicates));
    }

    private void parallelLoadFiles(List<File> files) {
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService service = Executors.newFixedThreadPool(threads);

        List<Future<Void>> futures = new ArrayList<>();
        for (final File file : files) {
            Callable<Void> callable = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    loadFile(file);
                    return null;
                }
            };
            futures.add(service.submit(callable));
        }
        service.shutdown();
    }

    private void loadFile(File currentFile) throws ParseException {
        String fileName = currentFile.getName();
        DateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
        Date fileDate = format.parse(fileName.split("_")[3]);
        System.out.println(fileDate);

        DateFormat logDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println(fileName + " Start - " + logDateFormat.format(new Date()));
        ingestFile(currentFile);
        System.out.println(fileName + " End - " + logDateFormat.format(new Date()));
    }

    private void ingestFile(File currentFile) {
        String currentLine;
        int saved = 0;
        DateFormat logDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        try (BufferedReader br = new BufferedReader(new FileReader(currentFile)); Connection repcon = this.reporting.getConnection()) {
            int lineCount = 0;

            while ((currentLine = br.readLine()) != null) {
                if (lineCount == 0) {
                    lineCount++;
                    continue;
                }

                CdrRow currentRow = new CdrRow();
                String[] properties = currentLine.split(",");
                currentRow.setSubscriptionId(properties[0].trim().split(":")[1].trim());
                currentRow.setPhoneNumber(properties[1].trim());
                currentRow.setAttemptNumber(Parser.parseInt(properties[3].trim()));
                currentRow.setStartTime(Parser.parseDate(properties[4].trim()));
                currentRow.setAnswerTime(Parser.parseDate(properties[5].trim()));
                currentRow.setEndTime(Parser.parseDate(properties[6].trim()));
                currentRow.setCallDurationInPulses(Parser.parseInt(properties[7].trim()));
                currentRow.setCallStatus(Parser.parseInt(properties[8].trim()));
                currentRow.setLlId(Parser.parseInt(properties[9].trim()));
                currentRow.setContentFilename(properties[10].trim());
                currentRow.setMsgStartTime(Parser.parseDate(properties[11].trim()));
                currentRow.setMsgEndTime(Parser.parseDate(properties[12].trim()));
                currentRow.setCircle(properties[13].trim());
                currentRow.setOperator(properties[14].trim());
                currentRow.setWeekId(properties[17].trim());

                if (saveRow(currentRow, repcon)) {
                    saved++;
                }
                lineCount++;
                if (lineCount % 10000 == 0) {
                    System.out.println(logDateFormat.format(new Date()) + " Progress: Read - " + lineCount + ", Saved - " + saved);
                }
            }
            System.out.println("Read " + lineCount + " lines from file: " + currentFile.getName());
            System.out.println("Saved " + saved + " call detail records");
            totalLines += lineCount;
            totalSaved += saved;
        } catch (IOException|SQLException ex) {
            System.out.println(ex.toString());
        }
    }

    private boolean saveRow(CdrRow cdrRow, Connection repcon) {
        SubscriptionInfo si = lookupCache.getSubscriptionInfo(cdrRow.getSubscriptionId());
        if (si == null) {
            return false;
        }

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat timeFormat = new SimpleDateFormat("HH:mm:00");
        DateFormat timeFormatStamp = new SimpleDateFormat("HH:mm:ss");
        DateFormat modificationFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // name casing here matches db columns. #donthatetheplayerhatethegame
        long Subscription_ID = si.getId();
        int Operator_ID = lookupCache.getOperatorId(cdrRow.getOperator());
        int Subscription_Pack_ID = si.getPackId();
        int Campaign_ID = lookupCache.getCampaignId(cdrRow.getWeekId());
        int Start_Date_ID = lookupCache.getDateId(dateFormat.format(cdrRow.getStartTime()));
        int End_Date_ID = lookupCache.getDateId(dateFormat.format(cdrRow.getEndTime()));
        int Start_Time_ID = lookupCache.getTimeId(timeFormat.format(cdrRow.getStartTime()));
        int End_Time_ID = lookupCache.getTimeId(timeFormat.format(cdrRow.getEndTime()));
        Integer State_ID = null;
        String Call_Status = lookupCache.getCallStatus(cdrRow.getCallStatus());
        long Duration = (cdrRow.getEndTime().getTime() - cdrRow.getStartTime().getTime()) / 1000;
        String Service_Option = null;
        int Percentage_Listened = percentageCalculator(cdrRow.getMsgStartTime(), cdrRow.getMsgEndTime(), cdrRow.getWeekId());
        String Call_Source = "OBD";
        String Subscription_Status = "ACTIVE";
        int Duration_In_Pulse = cdrRow.getCallDurationInPulses();
        String Call_Start_Time = timeFormatStamp.format(cdrRow.getStartTime());
        String Call_End_Time = timeFormatStamp.format(cdrRow.getEndTime());
        int Attempt_Number = cdrRow.getAttemptNumber();
        Timestamp Subscription_Start_Date = si.getStartDate();
        int msg_duration = getMessageDurationInSeconds(cdrRow.getMsgStartTime(), cdrRow.getMsgEndTime());

        // add the row in sql
        try {
            Statement statement = repcon.createStatement();
            String query = String.format(KilkariConstants.insertCdrRowSql, Subscription_ID, Operator_ID, Subscription_Pack_ID, Campaign_ID,
                    Start_Date_ID, End_Date_ID, Start_Time_ID, End_Time_ID, State_ID, Call_Status, Duration, Service_Option, Percentage_Listened,
                    Call_Source, Subscription_Status, Duration_In_Pulse, Call_Start_Time, Call_End_Time, Attempt_Number,
                    modificationFormat.format(Subscription_Start_Date), msg_duration, modificationFormat.format(new Date())); // change modification date?
            statement.executeUpdate(query);
        } catch (SQLException sqle) {
            // System.out.println("Could not add row: " + sqle.toString());
            if (sqle.toString().contains("Duplicate entry")) {
                totalDuplicates++;
                if (totalDuplicates % 10000 == 0) {
                    System.out.println("Duplicates: " + totalDuplicates);
                }
            }
            return false;
        }

        return true;
    }

    private int percentageCalculator(Date msgStart, Date msgEnd, String weekId) {
        if (msgStart == null || msgEnd == null) {
            return 0;
        }
        int contentDuration = lookupCache.getMessageDuration(weekId);
        return ((getMessageDurationInSeconds(msgStart, msgEnd)) / contentDuration) * 100;
    }

    private int getMessageDurationInSeconds(Date msgStart, Date msgEnd) {
        if (msgStart == null || msgEnd == null) {
            return 0;
        }

        return (int)(msgEnd.getTime() - msgStart.getTime()) / 1000;
    }
}
