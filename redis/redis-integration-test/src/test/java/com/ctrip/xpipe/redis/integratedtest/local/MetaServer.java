package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.integratedtest.console.app.MetaserverApp;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.ServerStartCmd;
import com.ctrip.xpipe.redis.meta.server.MetaServerApplication;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.foundation.DefaultFoundationService.DATA_CENTER_KEY;
import static com.ctrip.xpipe.redis.core.config.AbstractCoreConfig.KEY_ZK_ADDRESS;
import static com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig.KEY_CONSOLE_ADDRESS;

public class MetaServer {

    MetaServerInfo info;
    String idc; 
    public MetaServer(String idc, MetaServerInfo info, ScheduledExecutorService scheduled) {
        this.idc = idc;
        this.info = info;
        this.scheduled = scheduled;
    }

    ConfigurableApplicationContext ctx;
    public void start(String console, String zk, Map<String, DcInfo> dcInfos) throws Exception {
        System.setProperty("server.port", String.valueOf(info.port));
        System.setProperty("cat.client.enabled", "false");
        System.setProperty("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
        System.setProperty("meta.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");
        System.setProperty(DATA_CENTER_KEY, idc);
        System.setProperty(KEY_CONSOLE_ADDRESS, console);
        System.setProperty(KEY_ZK_ADDRESS, zk);
        System.setProperty(DefaultMetaServerConfig.KEY_DC_INFOS, JsonCodec.INSTANCE.encode(dcInfos));
        SpringApplication app = new SpringApplication(MetaServerApplication.class);
//        app.setBannerMode(Banner.Mode.OFF);
        ctx = app.run("");
        ctx.start();

    }
    
    ScheduledExecutorService scheduled;
    final static Logger logger = LoggerFactory.getLogger(MetaServer.class);
    public void start2(String console, String zk, Map<String, DcInfo> dcInfos) throws Exception {
        ServerStartCmd metaserver = new ServerStartCmd(idc + info.port, MetaserverApp.class.getName(), new HashMap<String, String>() {{
            put("server.port", String.valueOf(info.port));
            put("cat.client.enabled", "false");
            put("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
            put("meta.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");
            put(DATA_CENTER_KEY, idc);
            put(KEY_CONSOLE_ADDRESS, console);
            put(KEY_ZK_ADDRESS, zk);
            logger.info("metaserver {}", JsonCodec.INSTANCE.encode(dcInfos));
            put(DefaultMetaServerConfig.KEY_DC_INFOS, JsonCodec.INSTANCE.encode(dcInfos));
        }}, scheduled);
        metaserver.execute(scheduled).addListener(metaserverFuture -> {
            if (metaserverFuture.isSuccess()) {
                logger.info("[startMetaServer] metaserver {}-{} end {}", idc, info.port, metaserverFuture.get());
            } else {
                logger.info("[startMetaServer] metaserver {}-{} fail", idc, info.port, metaserverFuture.cause());
            }
        });

//        subProcessCmds.add(metaserver);
//        return metaserver;
    }
    
    public void stop() throws Exception {

    }

    

    static class MetaServerInfo {
        int port;
        public MetaServerInfo(int port) {
            this.port = port;
        }
        public String getAddess() {
            return "http://127.0.0.1:" + port;
        }
    }
    
    
}
