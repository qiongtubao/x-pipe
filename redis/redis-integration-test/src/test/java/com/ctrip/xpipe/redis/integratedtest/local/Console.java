package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.App;
import com.ctrip.xpipe.redis.integratedtest.ConsoleStart;
import com.ctrip.xpipe.redis.integratedtest.console.app.ConsoleApp;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.ServerStartCmd;
import com.ctrip.xpipe.redis.integratedtest.metaserver.AbstractMetaServerMultiDcTest;
import com.ctrip.xpipe.simpleserver.SimpleTestSpringServer;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.foundation.DefaultFoundationService.DATA_CENTER_KEY;
import static com.ctrip.xpipe.redis.checker.config.CheckerConfig.KEY_CHECKER_META_REFRESH_INTERVAL;
import static com.ctrip.xpipe.redis.checker.config.CheckerConfig.KEY_SENTINEL_CHECK_INTERVAL;
import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.KEY_SERVER_MODE;
import static com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig.KEY_METASERVERS;
import static com.ctrip.xpipe.redis.core.config.AbstractCoreConfig.KEY_ZK_ADDRESS;

public class Console {
    static final Logger logger = LoggerFactory.getLogger(Console.class);
    public static class ConsoleInfo {
        ConsoleServerModeCondition.SERVER_MODE mode;
        int console_port;
        int checker_port;
        boolean enable = true;
        
        public String getConsoleAddress() {
            return "http://127.0.0.1:" + this.console_port;
        }

        public ConsoleInfo(ConsoleServerModeCondition.SERVER_MODE mode) {
            this.mode = mode;
        }

        public ConsoleInfo setChecker_port(int checker_port) {
            this.checker_port = checker_port;
            return this;
        }

        public ConsoleInfo setConsole_port(int console_port) {
            this.console_port = console_port;
            return this;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }

        public boolean getEnable() {
            return enable;
        }

    }
    ConfigurableApplicationContext ctx;
    int consolePort;
    public void start(int port, String idc, String zk, List<String> localDcConsoles,
               Map<String, String> crossDcConsoles, Map<String, String> metaservers,
               Map<String, String> extras) throws Exception {
//        logger.info(remarkableMessage("[startConsoleServer]{}"), consolePort);
        System.setProperty(HealthChecker.ENABLED, "true");
        System.setProperty("server.port", String.valueOf(port));
        System.setProperty("cat.client.enabled", "false");
        System.setProperty("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
        System.setProperty(DATA_CENTER_KEY, idc);
        System.setProperty(KEY_ZK_ADDRESS, zk);
        System.setProperty(KEY_METASERVERS, JsonCodec.INSTANCE.encode(metaservers));
        System.setProperty("console.domains", JsonCodec.INSTANCE.encode(crossDcConsoles));
        System.setProperty("console.all.addresses", String.join(",", localDcConsoles));
        System.setProperty(KEY_CHECKER_META_REFRESH_INTERVAL, "2000");
        System.setProperty(KEY_SENTINEL_CHECK_INTERVAL, "15000");
//        putAll(extras);
        for(Map.Entry<String, String> entry: extras.entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue());
        }

        SpringApplication app = new SpringApplication(App.class);
//        app.setBannerMode(Banner.Mode.OFF);
        ctx = app.run("");
        ctx.start();
    }
    
    void start2(int port, String idc, String zk, List<String> localDcConsoles,
                Map<String, String> crossDcConsoles, Map<String, String> metaservers,
                Map<String, String> extras,
                ScheduledExecutorService scheduled) {
        ServerStartCmd consoleServer = new ServerStartCmd(idc + port, ConsoleApp.class.getName(), new HashMap<String, String>() {{
            put(HealthChecker.ENABLED, "true");
            put("server.port", String.valueOf(port));
            put("cat.client.enabled", "false");
            put("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
            put(DATA_CENTER_KEY, idc);
            put(KEY_ZK_ADDRESS, zk);
            put(KEY_METASERVERS, JsonCodec.INSTANCE.encode(metaservers));
            logger.info("crossDc {}", JsonCodec.INSTANCE.encode(crossDcConsoles));
            put("console.domains", JsonCodec.INSTANCE.encode(crossDcConsoles));
            put("console.all.addresses", String.join(",", localDcConsoles));
            put(KEY_CHECKER_META_REFRESH_INTERVAL, "2000");
            put(KEY_SENTINEL_CHECK_INTERVAL, "15000");
            putAll(extras);
        }}, scheduled);
        consoleServer.execute(scheduled).addListener(consoleFuture -> {
            if (consoleFuture.isSuccess()) {
                logger.info("[startConsoleJQ] console {}-{} end {}", idc, port, consoleFuture.get());
            } else {
                logger.info("[startConsoleJQ] console {}-{} fail", idc, port, consoleFuture.cause());
            }

        });

//        subProcessCmds.add(consoleServer);
//        return consoleServer;
    }

    private ComponentRegistry componentRegistry;
    protected void add(Object lifecycle) throws Exception {
        this.componentRegistry.initialize();
        this.componentRegistry.add(lifecycle);
    }


    void stop() throws Exception {
        this.componentRegistry.stop();
    }

}
