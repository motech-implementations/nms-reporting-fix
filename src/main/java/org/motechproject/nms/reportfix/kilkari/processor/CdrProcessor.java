package org.motechproject.nms.reportfix.kilkari.processor;

import org.motechproject.nms.reportfix.kilkari.cache.LookupCache;
import org.motechproject.nms.reportfix.kilkari.domain.CdrRow;
import org.motechproject.nms.reportfix.kilkari.helpers.Parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Processor to read cdr files and store them
 */
public class CdrProcessor {

    private LookupCache lookupCache;

    public CdrProcessor() {
    }

    public void startProcessor(String directoryPath, LookupCache lookupCache) throws ParseException {
        if (lookupCache == null) {
            return;
        }

        this.lookupCache = lookupCache;
        File directory = new File(directoryPath);
        for (File currentFile : directory.listFiles()) {
            ingestFile(currentFile);
        }
    }

    private void ingestFile(File currentFile) throws ParseException {
        String fileName = currentFile.getName();
        DateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
        Date fileDate = format.parse(fileName.split("_")[3]);
        System.out.println(fileDate);

        loadFile(currentFile);
    }

    private void loadFile(File currentFile) {
        String currentLine;

        try (BufferedReader br = new BufferedReader(new FileReader(currentFile))) {
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
                lineCount++;
            }
            System.out.println("Read " + lineCount + " lines from file: " + currentFile.getName());
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }
    }
}
