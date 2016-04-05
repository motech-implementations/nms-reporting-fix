package org.motechproject.nms.reportfix.kilkari.helpers;

import java.util.Date;

/**
 * Helper to get numbers from strings
 */
public final class Parser {

    public static Long parseLong(String l) {
        try {
            return Long.parseLong(l);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    public static Integer parseInt(String i) {
        try {
            return Integer.parseInt(i);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    public static Date parseDate(String d) {
        Long secondsSinceEpoch = parseLong(d);
        return secondsSinceEpoch == null ? null : new Date(secondsSinceEpoch * 1000);
    }
}
