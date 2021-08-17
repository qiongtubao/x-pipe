package com.ctrip.xpipe.redis.integratedtest.metaserver.scenes;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.integratedtest.metaserver.AbstractMetaServerMultiDcTest;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.tuple.Pair;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.RestOperations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 *  when route change
 *  peer proxy will change
 *  metaserver send peerof command
 */
public class RouteChangeTest extends AbstractMetaServerMultiDcTest {

    @Before
    public void testBefore() throws Exception {
        startCRDTAllServer();
        Thread.currentThread().join();
    }

    class ConsoleTaskFactory {
        String consoleServerUrl;
        String idc;
        XpipeNettyClientKeyedObjectPool pool;
        ScheduledExecutorService scheduled;
        public ConsoleTaskFactory(String idc, String url, XpipeNettyClientKeyedObjectPool pool, ScheduledExecutorService scheduled) {
            this.idc = idc;
            this.consoleServerUrl = url;
            this.pool = pool;
            this.scheduled = scheduled;
        }
        protected RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(3, 100);
        BooleanSupplier waitShardHealthTask(String clustername, String shardname) {
            String url = String.format("http://%s/console/cross-master/delay/bi_direction/%s/%s/%s", this.consoleServerUrl, this.idc, clustername, shardname);
            return () -> {
                String rest = restTemplate.getForObject(url, String.class);
                if(rest == null || rest.equals("{}")) {
                    return false;
                }
                JsonObject obj = new JsonParser().parse(rest).getAsJsonObject();
                if(obj == null || obj.entrySet().size() == 0) {
                    return false;
                }
                for(Map.Entry<String, JsonElement> entry: obj.entrySet()) {
                    JsonObject o = entry.getValue().getAsJsonObject();
                    for(Map.Entry<String, JsonElement> es: o.entrySet()) {
                        int r = es.getValue().getAsInt();
                        if(r < 0 && r > 10000) {
                            return false;
                        }
                    }
                }
                return true;
            };
        }

        BooleanSupplier checkPeerMaster(Endpoint master, Pair<Long, Endpoint> peer) {
            return () -> {
                CRDTInfoCommand crdtInfoCommand = new CRDTInfoCommand(pool.getKeyPool(master), InfoCommand.INFO_TYPE.REPLICATION ,scheduled);
                try {
                    CRDTInfoResultExtractor re = new CRDTInfoResultExtractor(crdtInfoCommand.execute().get());
                    List<Pair<Long, Endpoint>> peers =  re.extractPeerMasters();
                    for(Pair<Long, Endpoint> p: peers) {
                        if(p.getKey().equals(peer.getKey()) && p.getValue().equals(peer.getValue())) {
                            return true;
                        }
                        logger.info("gid: {} , endpoint {}", p.getKey(), p.getValue());
                    }
                    return false;
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            };
        }
    }

    ProxyEnabledEndpoint createProxyEndpoint(String host, int port, String proxy) {
        ProxyConnectProtocol protocol = new DefaultProxyConnectProtocolParser().read(proxy);
        return new ProxyEnabledEndpoint(host, port, protocol);
    }

    @Test
    public void testRouteChange() throws Exception {
        String jqConsoleUrl = "127.0.0.1:18080";
        String fraConsoleUrl = "127.0.0.1:18082";
        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
        Endpoint master = new DefaultEndPoint("127.0.0.1", 36379);
        ConsoleTaskFactory jqTaskFactory = new ConsoleTaskFactory("jq", jqConsoleUrl , pool, scheduled);
        waitConditionUntilTimeOut(jqTaskFactory.waitShardHealthTask("cluster1", "shard1"), 100000, 1000);
        ConsoleTaskFactory fraTaskFactory = new ConsoleTaskFactory("fra", fraConsoleUrl , pool, scheduled);
        waitConditionUntilTimeOut(fraTaskFactory.waitShardHealthTask("cluster1", "shard1"), 100000, 1000);
        waitConditionUntilTimeOut(fraTaskFactory.checkPeerMaster(master, new Pair<Long, Endpoint>(5L, createProxyEndpoint("127.0.0.1", 38379, "PROXY ROUTE PROXYTCP://127.0.0.1:11081 PROXYTLS://127.0.0.1:11443"))), 100000, 1000);
//        CheckerPeer p = new CheckerPeer(pool.getKeyPool(master), scheduled);
//        p.setHadPeerParams("127.0.0.1", 38379);
//        p.setProxySize("127.0.0.1", 38379, 2);
//        waitConditionUntilTimeOut(p::checkHadPeer, 100000, 1000);
//
//        PeerOfCommand peerOfCommand = new PeerOfCommand(pool.getKeyPool(master), getGid("fra"), null, 0, scheduled);
//        peerOfCommand.execute().get();
//        Assert.assertEquals(p.checkHadPeer(), false);
//        waitConditionUntilTimeOut(p::checkHadPeer, 100000, 1000);
//
//
//        Assert.assertEquals(p.checkProxySize(), true);
//        RouteModel model = new RouteModel();
//        model.setId(2).setSrcProxyIds("2").setTag("META").setDstProxyIds("1,5").setSrcDcName("jq").setDstDcName("fra").setActive(true);
//        ChangeRoute(jqConsoleUrl, model);
//        p.setProxySize("127.0.0.1", 38379,1);
//        waitConditionUntilTimeOut(p::checkProxySize, 100000, 1000);
//        model.setSrcProxyIds("2,6");
//        ChangeRoute(jqConsoleUrl, model);
//        p.setProxySize("127.0.0.1", 38379,2);
//        waitConditionUntilTimeOut(p::checkProxySize, 100000, 1000);
//
//        Endpoint master_point= new DefaultEndPoint("127.0.0.1", 38380);
//        Endpoint slave_point= new DefaultEndPoint("127.0.0.1", 38379);
//        Command<String> command = new DefaultSlaveOfCommand(
//                pool.getKeyPool(master_point),
//                scheduled);
//        command.execute();
//        command = new DefaultSlaveOfCommand(pool.getKeyPool(slave_point), master_point.getHost(), master_point.getPort(), scheduled);
//        command.execute();
//        p.setProxySize("127.0.0.1", 38380,2);
//        waitConditionUntilTimeOut(p::checkProxySize, 100000, 1000);

    }

    @After
    public void testAfter() {
        stopAllServer();
    }



}
