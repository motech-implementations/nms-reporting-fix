package org.motechproject.nms.reportfix.kilkari.importer;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.motechproject.nms.reportfix.kilkari.constants.KilkariConstants;
import org.motechproject.nms.reportfix.logger.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Class to import missing subscriptions from production into reporting
 */
public class SubscriptionImporter {

    private MysqlDataSource reporting;
    private MysqlDataSource production;
    private String directoryPath;
    private HashSet<String> missingSubscriptionSet;

    private int missing;
    private int found;

    public SubscriptionImporter() {
        this.missingSubscriptionSet = new HashSet<>();
        missing = 0;
        found = 0;
    }

    public void startImport(String directoryPath, MysqlDataSource production, MysqlDataSource reporting) {

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+5:30"));
        this.directoryPath = directoryPath;
        this.production = production;
        this.reporting = reporting;
        findMissingSubscriptions();
    }

    private void findMissingSubscriptions() {
        File directory = new File(this.directoryPath);
        List<File> files = Arrays.asList(directory.listFiles());
        Collections.sort(files);
        Logger.log(String.format("Found %d files", files.size()));

        int index = 1;
        for (File currentFile : files) {
            Logger.log(String.format("Loading file %d of %d", index, files.size()));
            try {
                loadFile(currentFile);
            } catch (ParseException pex) {
                Logger.log("Unable to read file: " + currentFile.getName());
            }

            index += 1;
            Logger.log("Current missing subscription count: " + missingSubscriptionSet.size());
        }

        Logger.log("Final missing subscription count: " + missingSubscriptionSet.size());
    }

    private void loadFile(File currentFile) throws ParseException {
        String fileName = currentFile.getName();
        DateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
        Date fileDate = format.parse(fileName.split("_")[3]);
        Logger.log(fileDate.toString());
        String currentLine;

        try (BufferedReader br = new BufferedReader(new FileReader(currentFile)); Connection repcon = this.reporting.getConnection()) {

            int lineCount = 0;

            while ((currentLine = br.readLine()) != null) {
                if (lineCount == 0) {
                    lineCount++;
                    continue;
                }

                if (lineCount % 100000 == 0) {
                    Logger.log(String.format("Read %d lines", lineCount));
                }

                String subscriptionId = currentLine.split(",")[0].trim().split(":")[1].trim();
                Statement statement = repcon.createStatement();
                String query = String.format(KilkariConstants.getSubscriptionRow, subscriptionId);
                ResultSet rs = statement.executeQuery(query);
                rs.next();
                if (rs.getInt(1) == 0) {
                    missingSubscriptionSet.add(subscriptionId);
                }
                lineCount++;
            }
        } catch (SQLException|IOException ex) {
            Logger.log(ex.toString());
        }
    }

}
