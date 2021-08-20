package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;
import com.ctrip.xpipe.zk.ZkTestServer;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Zk {
    static class ZkInfo {
        String idc;
        List<Integer> ports = Lists.newArrayList();
        
        public ZkInfo(String idc, ZkServerMeta meta) {
            this.idc = idc;
            String[] addresses = meta.getAddress().split("\\s*,\\s*");
            if (addresses.length != 1) {
                throw new IllegalStateException("zk server test should only be one there!" + meta.getAddress());
            }
            for(int i = 0; i < addresses.length; i++) {
                ports.add(Utils.getPort(addresses[i]));
            }
        }

        public List<Integer> getPorts() {
            return ports;
        }

        public String getIdc() {
            return idc;
        }

//        public String getAddress() {
//            return "127.0.0.1:" + port;
//        }
    }
    final Logger logger = LoggerFactory.getLogger(Zk.class);
    
    public ZkTestServer start(int zkPort) throws Exception {
        logger.info("start {}", zkPort);
        ZkTestServer zkTestServer = new ZkTestServer(zkPort);
        zkTestServer.initialize();
        zkTestServer.start();
        return zkTestServer;
    }

}
