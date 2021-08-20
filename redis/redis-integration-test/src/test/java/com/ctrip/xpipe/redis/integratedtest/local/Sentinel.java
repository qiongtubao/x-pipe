package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisKillCmd;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisStartCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public class Sentinel implements Lifecycle {
    @Override
    public void dispose() throws Exception {
        
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public LifecycleState getLifecycleState() {
        return null;
    }

    @Override
    public void start() throws Exception {
        RedisStartCmd cmd = new RedisStartCmd(info.endpoint.getPort(), true, scheduled);
        cmd.execute(scheduled);
    }

    @Override
    public void stop() throws Exception {
        new RedisKillCmd(info.endpoint.getPort(), scheduled).execute().get();
    }

    @Override
    public int getOrder() {
        return 0;
    }
    final Logger logger = LoggerFactory.getLogger(Sentinel.class);
    static class SentinelInfo {
        Endpoint endpoint;
        public SentinelInfo(Endpoint endpoint) {
            this.endpoint = endpoint;
        }
    }
    SentinelInfo info;
    ScheduledExecutorService scheduled;
    public Sentinel(SentinelInfo sentinelInfo, ScheduledExecutorService scheduled) {
        this.info = sentinelInfo;
        this.scheduled = scheduled;
    }
    
    
}
