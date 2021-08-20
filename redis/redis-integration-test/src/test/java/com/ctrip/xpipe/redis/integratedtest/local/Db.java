package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.apache.logging.log4j.util.Strings;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Db {
    protected final String KEY_H2_PORT = "h2Port";
    private Server h2Server;
    private String data_file;
    public Db(String data_file) {
        this.data_file = data_file;
    }
    void startH2Server() throws SQLException {
        int h2Port = Integer.parseInt(System.getProperty(KEY_H2_PORT, "9123"));
        h2Server = Server.createTcpServer("-tcpPort", String.valueOf(h2Port), "-tcpAllowOthers");
        h2Server.start();
    }

    Logger logger = LoggerFactory.getLogger(Db.class);
    public static String DATA_SOURCE = "fxxpipe";

//    public static final String DEMO_DATA = "xpipe-crdt.sql";
    public static final String TABLE_STRUCTURE = "sql/h2/xpipedemodbtables.sql";
    public static final String TABLE_DATA = "sql/h2/xpipedemodbinitdata.sql";
    protected String[] dcNames = new String[]{"jq", "oy"};

    protected void executeSqlScript(String prepareSql) throws ComponentLookupException, SQLException {

        DataSourceManager dsManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);

        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = dsManager.getDataSource(DATA_SOURCE).getConnection();
            conn.setAutoCommit(false);
            if (!Strings.isEmpty(prepareSql)) {
                for (String sql : prepareSql.split(";")) {
                    String executeSql = sql.trim();
                    if (StringUtil.isEmpty(executeSql)) continue;

                    logger.debug("[setup][data]{}", executeSql);
                    stmt = conn.prepareStatement(executeSql);
                    stmt.executeUpdate();
                }
            }
            conn.commit();

        } catch (Exception ex) {
            logger.error("[SetUpTestDataSource][fail]:", ex);
            if (null != conn) {
                conn.rollback();
            }
        } finally {
            if (null != stmt) {
                stmt.close();
            }
            if (null != conn) {
                conn.setAutoCommit(true);
                conn.close();
            }
        }
    }

    void setUpTestDataSource() throws IOException, SQLException, ComponentLookupException {
        DataSourceManager dsManager = ContainerLoader.getDefaultContainer().lookup(DataSourceManager.class);
        DataSource dataSource = null;
        try {
            dataSource = dsManager.getDataSource(DATA_SOURCE);
        } catch(Exception e) {
            logger.info("[setUpTestDataSource][ignore if it it not console]{}", e.getMessage());
            return;
        }

        String driver = dataSource.getDescriptor().getProperty("driver", null);
        if (driver != null && driver.equals("org.h2.Driver")) {
            executeSqlScript(FileUtils.readFileAsString(TABLE_STRUCTURE));
            executeSqlScript(FileUtils.readFileAsString(data_file));

        } else {
            logger.info("[setUpTestDataSource][do not clean]{}", driver);
        }
    }

    void start() throws SQLException, ComponentLookupException, IOException {
        startH2Server();
        setUpTestDataSource(); // init data in h2
    }
}
