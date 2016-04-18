package org.motechproject.nms.reportfix.logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Logger helper to write to stream
 */
public final class Logger {
    private static DateFormat logDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    public static void log(String logLine) {

        System.out.println(String.format("%s: %s", logDateFormat.format(new Date()), logLine));
    }
}
