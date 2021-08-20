package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.SlaveOfCommand;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.CrdtRedisStartCmd;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisKillCmd;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisStartCmd;
import com.ctrip.xpipe.utils.StringUtil;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public class Redis implements Lifecycle {
    ClusterType type;
    RedisInfo redisInfo;
    int gid;
    ScheduledExecutorService scheduled;
    static Logger logger = LoggerFactory.getLogger(Redis.class);
    
    static List<RedisStartCmd> onliveRedis = Lists.newArrayList();

    XpipeNettyClientKeyedObjectPool pool;
    public Redis(int gid, ClusterType type, RedisInfo redisInfo, XpipeNettyClientKeyedObjectPool pool, ScheduledExecutorService scheduled) {
        this.redisInfo = redisInfo;
        this.type = type;
        this.gid = gid;
        this.scheduled = scheduled;
        this.pool = pool;
    }

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
        logger.info("redis {} start", redisInfo.meta.getPort());
        RedisStartCmd cmd;
        if(this.type.supportMultiActiveDC()) {
            cmd = new CrdtRedisStartCmd(gid, redisInfo.meta.getPort(), scheduled);
        } else {
            cmd = new RedisStartCmd(redisInfo.meta.getPort(), scheduled);
        }
        cmd.execute(scheduled);
        String masterStr = redisInfo.meta.getMaster();
        if(!StringUtil.isEmpty(masterStr)) {
            logger.info("{} {}", redisInfo.meta.getIp(), redisInfo.meta.getPort());
            Endpoint master = Utils.getEndpoint(masterStr);
            Endpoint slave = new DefaultEndPoint(redisInfo.meta.getIp(), redisInfo.meta.getPort());
            new SlaveOfCommand(pool.getKeyPool(slave), master.getHost(), master.getPort(), scheduled).execute(scheduled);
        }
        onliveRedis.add(cmd);
    }

    @Override
    public void stop() throws Exception {
        new RedisKillCmd(redisInfo.meta.getPort(), scheduled).execute().get();
    }

    @Override
    public int getOrder() {
        return 0;
    }

    static class RedisInfo {
        RedisMeta meta;
        public RedisInfo(RedisMeta meta) {
            this.meta = meta;
        }
    }
}
