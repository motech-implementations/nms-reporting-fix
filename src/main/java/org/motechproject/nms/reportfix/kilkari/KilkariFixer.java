package org.motechproject.nms.reportfix.kilkari;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by kosh on 3/29/16.
 */
public class KilkariFixer {

    private MysqlDataSource production;
    private MysqlDataSource reporting;

    public KilkariFixer(MysqlDataSource prod, MysqlDataSource reporting) {
        this.production = prod;
        this.reporting = reporting;
    }

    public void start() {
        Connection prod = null;
        System.out.println("Fixing Kilkari data");
        try {
            prod = production.getConnection();
        } catch (SQLException sqle) {
            System.out.println("Unable to connect to motech db: " + sqle.toString());
            return;
        } // todo: close connection?
    }
}
