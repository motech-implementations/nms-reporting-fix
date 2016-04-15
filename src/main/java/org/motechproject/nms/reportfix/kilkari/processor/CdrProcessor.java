package org.motechproject.nms.reportfix.kilkari.processor;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.kilkari.cache.LookupCache;
import org.motechproject.nms.reportfix.kilkari.constants.KilkariConstants;
import org.motechproject.nms.reportfix.kilkari.domain.CdrRow;
import org.motechproject.nms.reportfix.kilkari.domain.SubscriptionInfo;
import org.motechproject.nms.reportfix.kilkari.helpers.Parser;
import org.motechproject.nms.reportfix.logger.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
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
        Date startTime = new Date();

        // no lookup, nothing to do
        if (lookupCache == null) {
            return;
        }

        // set timezone
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+5:30"));

        // set data sources
        this.lookupCache = lookupCache;
        this.reporting = reporting;

        // disable safe updates
        toggleSafeUpdate(reporting, false);

        // start processing files
        File directory = new File(directoryPath);
        List<File> files = Arrays.asList(directory.listFiles());
        Collections.sort(files);
        Logger.log(String.format("Found %d files", files.size()));

        // parallelLoadFiles(files);

        int index = 1;
        for (File currentFile : files) {
            Logger.log(String.format("Loading file %d of %d", index, files.size()));
            loadFile(currentFile);
            index += 1;
        }

        Date endTime = new Date();
        Logger.log("Start: " + startTime.toString() + " End: " + endTime.toString());
        Logger.log(String.format("%s processed. Total records: %d, Saved: %d, Duplicates: %d", directoryPath, totalLines, totalSaved, totalDuplicates));

        // re-enable safe updates
        toggleSafeUpdate(reporting, true);
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

    /**
     * Used to load the file and delete any existing records for the day from db
     * @param currentFile current file to process
     * @throws ParseException
     */
    private void loadFile(File currentFile) throws ParseException {

        // delete existing cdrs for date in CDR file
        String fileName = currentFile.getName();
        String cdrDate = fileName.split("_")[3];
        cdrDate = cdrDate.substring(0, cdrDate.indexOf(".csv"));
        DateFormat parseDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        DateFormat lookupDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        deleteCdrForFileDate(lookupDateFormat.format(parseDateFormat.parse(cdrDate)));

        // ingest data from CDR file
        DateFormat logDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Logger.log(fileName + " Start - " + logDateFormat.format(new Date()));
        ingestFile(currentFile);
        Logger.log(fileName + " End - " + logDateFormat.format(new Date()));
    }

    /**
     * Process the file line-by-line and create a cdr row to save
     * @param currentFile file to process
     */
    private void ingestFile(File currentFile) {
        String currentLine;
        int saved = 0;
        DateFormat logDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date fileCreationTime = new Date();
        try {
            fileCreationTime = new Date(Files.readAttributes(currentFile.toPath(), BasicFileAttributes.class).creationTime().toMillis());
            Logger.log("Using file creation time: " + fileCreationTime.toString());
        } catch (IOException ioe) {
            Logger.log("Unable to determine file creation time. Using current time as modification date");
        }


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

                if (saveRow(currentRow, repcon, fileCreationTime)) {
                    saved++;
                }
                lineCount++;
                if (lineCount % 10000 == 0) {
                    Logger.log(logDateFormat.format(new Date()) + " Progress: Read - " + lineCount + ", Saved - " + saved);
                }
            }
            Logger.log("Read " + lineCount + " lines from file: " + currentFile.getName());
            Logger.log("Saved " + saved + " call detail records");
            totalLines += lineCount;
            totalSaved += saved;
        } catch (IOException|SQLException ex) {
            Logger.log(ex.toString());
        }
    }

    /**
     * Take a cdr row and save it to the subscr
     * @param cdrRow row to save
     * @param repcon connection to the reporting db
     * @param modificationDate modification date to use. Usually the file creation time here or ETL uses prod entity modification date
     * @return true if saved, false otherwise
     */
    private boolean saveRow(CdrRow cdrRow, Connection repcon, Date modificationDate) {
        SubscriptionInfo si = lookupCache.getSubscriptionInfo(cdrRow.getSubscriptionId());
        if (si == null) {
            return false;
        }

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat timeFormat = new SimpleDateFormat("HH:mm:00");
        DateFormat timeFormatStamp = new SimpleDateFormat("HH:mm:ss");
        DateFormat modificationFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String weekId = cdrRow.getContentFilename().substring(0, cdrRow.getContentFilename().indexOf(".wav"));

        // name casing here matches db columns. #donthatetheplayerhatethegame
        long Subscription_ID = si.getId();
        int Operator_ID = lookupCache.getOperatorId(cdrRow.getOperator());
        int Subscription_Pack_ID = si.getPackId();
        int Campaign_ID = lookupCache.getCampaignId(weekId);
        int Start_Date_ID = lookupCache.getDateId(dateFormat.format(cdrRow.getStartTime()));
        int End_Date_ID = lookupCache.getDateId(dateFormat.format(cdrRow.getEndTime()));
        int Start_Time_ID = lookupCache.getTimeId(timeFormat.format(cdrRow.getStartTime()));
        int End_Time_ID = lookupCache.getTimeId(timeFormat.format(cdrRow.getEndTime()));
        Integer State_ID = null;
        String Call_Status = lookupCache.getCallStatus(cdrRow.getCallStatus());
        long Duration = (cdrRow.getEndTime().getTime() - cdrRow.getStartTime().getTime()) / 1000;
        String Service_Option = null;
        int Percentage_Listened = percentageCalculator(cdrRow.getMsgStartTime(), cdrRow.getMsgEndTime(), weekId);
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
                    modificationFormat.format(Subscription_Start_Date), msg_duration, modificationFormat.format(modificationDate)); // change modification date?
            statement.executeUpdate(query);
        } catch (SQLException sqle) {
            // Logger.log("Could not add row: " + sqle.toString());
            if (sqle.toString().contains("Duplicate entry")) {
                totalDuplicates++;
                if (totalDuplicates % 10000 == 0) {
                    Logger.log("Duplicates: " + totalDuplicates);
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

    private void toggleSafeUpdate(MysqlDataSource dataSource, boolean value) {
        try (Connection repcon = this.reporting.getConnection()) {
            Statement statement = repcon.createStatement();
            String query = String.format(KilkariConstants.setSafeUpdates, value ? 1 : 0);
            statement.executeQuery(query);
        } catch (SQLException sqe) {
            Logger.log("Failed to toggle safe update to: " + value);
        }
    }

    private void deleteCdrForFileDate(String fileDate) {
        int Start_Date_ID = lookupCache.getDateId(fileDate);
        Logger.log(String.format("Trying to delete records with date: %s and Start_Date_ID: %d", fileDate, Start_Date_ID));
        try (Connection repcon = this.reporting.getConnection()) {
            String query = String.format(KilkariConstants.deleteRecordsForDay, Start_Date_ID);
            PreparedStatement statement = repcon.prepareStatement(query);
            int deleted = statement.executeUpdate();
            Logger.log(String.format("Deleted %d records with date: %s and Start_Date_ID: %d", deleted, fileDate, Start_Date_ID));
        } catch (SQLException sqle) {
            Logger.log("Could not delete subscriber call measure for day: ");
        }
    }
}
