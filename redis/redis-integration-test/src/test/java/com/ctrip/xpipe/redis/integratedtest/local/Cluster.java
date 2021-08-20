package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.apache.commons.collections.ListUtils;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Cluster implements Lifecycle {
    ClusterInfo clusterInfo;
    
    ScheduledExecutorService scheduled;

    XpipeNettyClientKeyedObjectPool pool;
    public Cluster(ClusterInfo clusterInfo, XpipeNettyClientKeyedObjectPool pool, ScheduledExecutorService scheduled, Map<String, Integer> gids) {
        this.clusterInfo = clusterInfo;
        this.scheduled = scheduled;
        this.gids = gids;
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
    public void stop() throws Exception {
        clusterInfo.groups.forEach((groupName, groupInfo) -> {
            logger.info("group {}", groupName);
            groupInfo.ids.forEach((idc, redisInfoList) -> {
                logger.info("idc {} ", idc);
                redisInfoList.forEach(redisInfo ->  {
                    try {
                        new Redis(gids.get(idc), clusterInfo.type,  redisInfo, pool, scheduled).stop();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

            });
        });
    }

    @Override
    public int getOrder() {
        return 0;
    }

    Logger logger = LoggerFactory.getLogger(Cluster.class);
    
    Map<String, Integer> gids;

    @Override
    public void start() throws Exception {
        clusterInfo.groups.forEach((groupName, groupInfo) -> {
            logger.info("group {}", groupName);
            groupInfo.ids.forEach((idc, redisInfoList) -> {
                logger.info("idc {} ", idc);
                redisInfoList.forEach(redisInfo ->  {
                    try {
                        new Redis(gids.get(idc), clusterInfo.type, redisInfo, pool, scheduled).start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                
            });
        });
        
    }

    static class GroupInfo {
        Map<String, List<Redis.RedisInfo>> ids = Maps.newConcurrentMap();

        public void addRedises(String idc, List<RedisMeta> meta) {
            ids.put(idc, meta.stream().map(redisMeta -> new Redis.RedisInfo(redisMeta)).collect(Collectors.toList()));
        } 
    }
    static class ClusterInfo {
        Map<String, GroupInfo> groups = Maps.newConcurrentMap();
        List<String> dcs = Lists.newArrayList();
        ClusterType type;
        void addShard(String idc, String shardName,  ShardMeta shardMeta) {
            List<RedisMeta> redises =  shardMeta.getRedises();
            GroupInfo groupInfo = MapUtils.getOrCreate(groups, shardName, new ObjectFactory<GroupInfo>() {
                @Override
                public GroupInfo create() {
                    return new GroupInfo();
                }
            });
            groupInfo.addRedises(idc, redises);
        }

        void CheckOrSetType(String clusterName, ClusterType type) {
            if(this.type == null) {
                this.type = type;
            } else if(type != this.type) {
                throw new RuntimeException(String.format("cluster type error %s", clusterName));
            }
        }

        void CheckOrDcs(String clusterName, String dcsStr) {
            List<String> dcs = Arrays.stream(dcsStr.split("\\s*,\\s*")).collect(Collectors.toList());
            if(this.dcs.size() == 0) {
                dcs.forEach((dc) -> {
                    if(this.dcs.contains(dc)) {
                        throw  new RuntimeException(String.format("cluster %s dc contains %s", clusterName, dc));
                    } else {
                        this.dcs.add(dc);
                    }
                });
            } else {
                if(dcs.size() != this.dcs.size()) {
                    throw new RuntimeException(String.format("cluster dcs %s != %s", this.dcs, dcs));
                }
                for(String dc: dcs) {
                    if(!this.dcs.contains(dc)) {
                        throw  new RuntimeException(String.format("%s cluster dc err %s", clusterName, dc));
                    }
                }
            }
        }
        public void addIdc(String idc, String clusterName, ClusterMeta clusterMeta) {
            ClusterType type = ClusterType.lookup(clusterMeta.getType());
            CheckOrSetType(clusterName, type);
            if(type.supportMultiActiveDC()) {
                CheckOrDcs(clusterName, clusterMeta.getDcs());
            }
            clusterMeta.getShards().forEach((shardName, shardMeta) -> {
                addShard(idc, shardName, shardMeta);
            });
        }
    }
    
   
    
    
    
}
