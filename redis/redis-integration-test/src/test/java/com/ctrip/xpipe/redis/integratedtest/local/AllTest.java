package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.model.RouteModel;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.proxy.parser.route.RouteOptionParser;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Maps;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestOperations;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.cluster.AbstractCheckerLeaderElector.KEY_CHECKER_ID;
import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.KEY_SERVER_MODE;
import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.SERVER_MODE.*;
import static com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig.KEY_CONSOLE_ADDRESS;

public class AllTest  {
    ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), XpipeThreadFactory.create("local.test"));

    public AllTest() throws Exception {
    }

    public Map<String, Console.ConsoleInfo> defaultConsoleInfo() {
        Map<String, Console.ConsoleInfo> consoleInfos = new HashMap<>();
        //start console + checker 2server
        consoleInfos.put("jq", new Console.ConsoleInfo(CONSOLE).setConsole_port(18080).setChecker_port(28080));
        //start conset_checker 1server
        consoleInfos.put("oy", new Console.ConsoleInfo(CONSOLE_CHECKER).setConsole_port(18081).setChecker_port(28081));
        //start checker 1 server
        consoleInfos.put("fra", new Console.ConsoleInfo(CHECKER).setConsole_port(18080).setChecker_port(28082));
        return consoleInfos;
    }
    
    Map<String,  Console.ConsoleInfo> consoles = defaultConsoleInfo();

    Map<String, String> getConsoleMap() {
        Map<String, String> cs = new ConcurrentHashMap<>();
        consoles.forEach((key, value) -> {
            cs.put(key, value.getConsoleAddress());
        });
        return cs;
    }
    
    Map<String, Zk.ZkInfo> zks = new ConcurrentHashMap<>();
    Map<String, String> extraOptions;
    Map<String, MetaServer.MetaServerInfo> metaServers = new ConcurrentHashMap<>();
    Map<String, Cluster.ClusterInfo> clusters = new ConcurrentHashMap<>();

    Map<String, String> getMetaServerMap() {
        Map<String, String> ms = new ConcurrentHashMap<>();
        metaServers.forEach((key, value) -> {
            ms.put(key, value.getAddess());
        });
        return ms;
    }
    
    void parseZk(String idc, ZkServerMeta meta) {
        zks.put(idc, new Zk.ZkInfo(idc, meta));
    }
    
    
    void parseMetaServer(String idc, List<MetaServerMeta> list) {
        metaServers.put(idc, new MetaServer.MetaServerInfo(list.get(0).getPort()));
    }
    
    void parseCluster(String idc, Map<String, ClusterMeta> metas) {
        metas.forEach((clusterName, clusterMeta) -> {
            Cluster.ClusterInfo clusterInfo = MapUtils.getOrCreate(clusters, clusterName, new ObjectFactory<Cluster.ClusterInfo>() {
                @Override
                public Cluster.ClusterInfo create() {
                    return new Cluster.ClusterInfo();
                }
            });
            clusterInfo.addIdc(idc, clusterName, clusterMeta);
        });
        
    }
    
    Map<String, Pair<List<Endpoint>, List<Endpoint>>> proxyPoints = Maps.newConcurrentMap();
    void parseProxy(String idc, List<RouteMeta> routes) {
        routes.forEach(route-> {
            String srcDc = route.getSrcDc();
            String dstDc = route.getDstDc();
            RouteOptionParser parser = new RouteOptionParser();
            parser.read("ROUTE " + route.getRouteInfo());
            List<ProxyEndpoint> srcEndpoints = parser.getNextEndpoints();
            parser.removeNextNodes();
            parser.read(parser.output());
            List<ProxyEndpoint> dscEndpoints = parser.getNextEndpoints();
            
            Pair<List<Endpoint>, List<Endpoint>> srcPair = MapUtils.getOrCreate(proxyPoints, srcDc,  () -> 
                new Pair<>(Lists.newArrayList(), Lists.newArrayList())
            );
            srcPair.getKey().addAll(srcEndpoints);
            
            Pair<List<Endpoint>, List<Endpoint>> dstPair = MapUtils.getOrCreate(proxyPoints, dstDc, () -> 
                new Pair<>(Lists.newArrayList(), Lists.newArrayList())
            );
            
            dstPair.getValue().addAll(dscEndpoints);
            
        });
    }

    Map<String, List<Sentinel.SentinelInfo>> allSentinels = Maps.newConcurrentMap();
    void parseSentinel(String idc, Map<Long, SentinelMeta> sentinels) {
        List<Sentinel.SentinelInfo> endpoints = Lists.newArrayList();
        sentinels.forEach((id, sentinelMeta) -> {
            endpoints.addAll(Arrays.stream(sentinelMeta.getAddress().split("\\s*(,|;)\\s*")).map(address -> 
                new Sentinel.SentinelInfo(Utils.getEndpoint(address))).collect(Collectors.toList()));
        });
        allSentinels.put(idc, endpoints);
    }
    
    void parseDcMeta(String idc, DcMeta dcMeta) {
        parseMetaServer(idc, dcMeta.getMetaServers());
        parseZk(idc, dcMeta.getZkServer());
        parseCluster(idc, dcMeta.getClusters());
        parseProxy(idc, dcMeta.getRoutes());
        parseSentinel(idc, dcMeta.getSentinels());
    }
    
    
   
    

    String getXml() {
        return "./local/local.xml";
    }

    Map<String, Integer> gids = new ConcurrentHashMap();
    protected void loadXml() throws Exception {
        MetaManager metaManager = new MetaManager(getXml());
        metaManager.initialize();
        XpipeMeta xpipeMeta = metaManager.getXpipeMeta();
        AtomicInteger gid = new AtomicInteger(1);
        xpipeMeta.getDcs().forEach((dc, meta) -> {
            parseDcMeta(dc, meta);
            gids.put(dc, gid.get());
            gid.incrementAndGet();
        });
        
        
    }
    
    final String JQ = "jq";
    final String OY = "oy";
    final String FRA = "fra";
    @Before
    public void init() throws Exception {
        loadXml();
        extraOptions = new HashMap<>();
        extraOptions.put("console.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");
    }

    @Test
    public void startDb() throws Exception {
        Db db = new Db("./local/local.sql");
        db.start();
        Zk zk = new Zk();
        for(Zk.ZkInfo zkinfo: zks.values()) {
            zkinfo.getPorts().forEach((port) -> {
                try {
                    zk.start(port);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        AtomicInteger proxyProxy = new AtomicInteger(9000);
        proxyPoints.forEach((idc, pairs) -> {
            List<Endpoint> keys = pairs.getKey();
            List<Endpoint> values = pairs.getValue();
            int keySize = keys.size();
            int valueSize = values.size();
            int min = Math.min(keySize, valueSize);
            for(int i = 0; i< min; i++) {
                try {
                    logger.info("start proxy {} {}", keys.get(i).getPort(), values.get(i).getPort());
                    new Proxy(new Proxy.ProxyInfo(keys.get(i).getPort(), values.get(i).getPort())).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("");
            if(keySize > valueSize) {
                for(int i = min; i < keySize; i++) {
                    try {
                        logger.info("start proxy {} {}", keys.get(i).getPort(), values.get(i).getPort());
                        new Proxy(new Proxy.ProxyInfo(keys.get(i).getPort(), proxyProxy.getAndIncrement())).start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else if(keySize < valueSize) {
                for(int i = min; i < valueSize; i++) {
                    try {
                        logger.info("start proxy {} {}", keys.get(i).getPort(), values.get(i).getPort());
                        new Proxy(new Proxy.ProxyInfo(proxyProxy.getAndIncrement(), values.get(i).getPort())).start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        });

        clusters.forEach((clusterName, clusterInfo) -> {
            try {
                new Cluster(clusterInfo, pool, scheduled, gids).start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        allSentinels.forEach((idc, sentinels) -> {
            sentinels.forEach(sentinelInfo-> {
                try {
                    new Sentinel(sentinelInfo, scheduled).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }) ;
        });
        Thread.currentThread().join();
    }

    protected XpipeNettyClientKeyedObjectPool getXpipeNettyClientKeyedObjectPool() throws Exception {
        XpipeNettyClientKeyedObjectPool result;
        result = new XpipeNettyClientKeyedObjectPool();
        LifecycleHelper.initializeIfPossible(result);
        LifecycleHelper.startIfPossible(result);
        return result;
    }
    
    XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();
    @Test
    public void startAllRedis() throws Exception {
        Thread.currentThread().join();
    }
    
    @Test
    public void closeAllRedis() throws Exception {
        clusters.forEach((clusterName, clusterInfo) -> {
            try {
                new Cluster(clusterInfo, pool, scheduled, gids).stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        allSentinels.forEach((idc, sentinels) -> {
            sentinels.forEach(sentinelInfo-> {
                try {
                    logger.info("{}", sentinelInfo.endpoint.getPort());
                    new Sentinel(sentinelInfo, scheduled).stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }) ;
        });
    }
    
    
    static final Logger logger = LoggerFactory.getLogger(AllTest.class);
    
    protected void startChecker(String idc) throws Exception {
        Console console = new Console();
        
        
        System.setProperty(KEY_SERVER_MODE, CHECKER.name());


        int zkPort = zks.get(idc).getPorts().get(0);
        List<String> consoleList = Lists.newArrayList();
        consoles.forEach((dcName, consoleInfo) -> {
            if(consoleInfo.console_port != 0) {
                consoleList.add("127.0.0.1:" + consoleInfo.console_port);
            }
        });
        Console.ConsoleInfo consoleInfo = consoles.get(idc);
        int port;
        port = consoleInfo.checker_port;
        
        ConcurrentHashMap options = new ConcurrentHashMap<>(extraOptions);
        options.put(KEY_CONSOLE_ADDRESS, "http://127.0.0.1:" + consoles.get(idc).console_port);
        options.put(KEY_CHECKER_ID, idc + port);
        console.start(port, idc, "127.0.0.1:" + zkPort , Collections.singletonList("127.0.0.1:" + consoleInfo.console_port), getConsoleMap(), getMetaServerMap(), options);
    }
    
    protected void startConsole(String idc, ConsoleServerModeCondition.SERVER_MODE mode) throws Exception {
        Console console = new Console();
        if (mode == null) {
            System.setProperty(KEY_SERVER_MODE, defaultConsoleInfo().get(idc).mode.name());
        } else {
            System.setProperty(KEY_SERVER_MODE, mode.name());
        }
        
        int zkPort = zks.get(idc).getPorts().get(0);
        List<String> consoleList = Lists.newArrayList();
        consoles.forEach((dcName, consoleInfo) -> {
            if(consoleInfo.console_port != 0) {
                consoleList.add("127.0.0.1:" + consoleInfo.console_port);
            }
        });
        Console.ConsoleInfo consoleInfo = consoles.get(idc);
        int port; 
        if(consoleInfo.console_port != 0) {
            port = consoleInfo.console_port;
        } else {
            port = consoleInfo.checker_port;
        }
        console.start(port, idc, "127.0.0.1:" + zkPort , Collections.singletonList("127.0.0.1:" + consoleInfo.console_port), getConsoleMap(), getMetaServerMap(), extraOptions);
    }
    
    @Test
    public void startJQConsole() throws Exception {
        startConsole(JQ, null);
        Thread.currentThread().join();
    }

    public String getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String pid = name.split("@")[0];
        return pid;
    }
    
    @Test
    public void startJQConsole2() throws Exception {
        startConsole2(JQ, null);
        System.out.println("run " + getPid());
        Thread.currentThread().join();
    }


    protected void startConsole2(String idc, ConsoleServerModeCondition.SERVER_MODE mode) throws Exception {
        Console console = new Console();
        Map<String, String> ops  = new ConcurrentHashMap<>(extraOptions);
        
        if (mode == null) {
            mode = defaultConsoleInfo().get(idc).mode;
        } 
        ops.put(KEY_SERVER_MODE, mode.name());

        int zkPort = zks.get(idc).getPorts().get(0);
        
        Console.ConsoleInfo consoleInfo = consoles.get(idc);
        int port = 0;
        switch (consoleInfo.mode) {
            case CONSOLE_CHECKER:
            case CONSOLE:
                port = consoleInfo.console_port;
                break;
            case CHECKER:
                port = consoleInfo.checker_port;
                break;
        }
        console.start2(port, idc, "127.0.0.1:" + zkPort , Collections.singletonList("127.0.0.1:" + consoleInfo.console_port), getConsoleMap(), getMetaServerMap(), ops, scheduled);
    }
    
    @Test
    public void startJQChecker() throws Exception {
        startChecker2(JQ);
        Thread.currentThread().join();
    }
    
    public void  startChecker2(String idc) throws Exception {
        Console console = new Console();

        int zkPort = zks.get(idc).getPorts().get(0);
        List<String> consoleList = Lists.newArrayList();
        consoles.forEach((dcName, consoleInfo) -> {
            if(consoleInfo.console_port != 0) {
                consoleList.add("127.0.0.1:" + consoleInfo.console_port);
            }
        });
        Console.ConsoleInfo consoleInfo = consoles.get(idc);
        int port;
        port = consoleInfo.checker_port;

        ConcurrentHashMap options = new ConcurrentHashMap<>(extraOptions);
        options.put(KEY_CONSOLE_ADDRESS, "http://127.0.0.1:" + consoles.get(idc).console_port);
        options.put(KEY_CHECKER_ID, idc + port);
        options.put(KEY_SERVER_MODE, CHECKER.name());
        console.start2(port, idc, "127.0.0.1:" + zkPort , Collections.singletonList("127.0.0.1:" + consoleInfo.console_port), getConsoleMap(), getMetaServerMap(), options, scheduled);
    }
    
    
    public void startMetaServer2(String idc) throws Exception {
        MetaServer metaServer = new MetaServer(idc, metaServers.get(idc), scheduled);
        int console_port = consoles.get(idc).console_port;
        int zkPort = zks.get(idc).getPorts().get(0);
        Map<String, DcInfo> dcInfos = new ConcurrentHashMap<>();
        metaServers.forEach((dc, metaServerInfo) -> {
            dcInfos.put(dc, new DcInfo(metaServerInfo.getAddess()));
        });
        metaServer.start2("http://127.0.0.1:" + console_port, "127.0.0.1:" + zkPort, dcInfos);
    }

    public void startMetaServer(String idc) throws Exception {
        MetaServer metaServer = new MetaServer(idc, metaServers.get(idc), scheduled);
        int console_port = consoles.get(idc).console_port;
        int zkPort = zks.get(idc).getPorts().get(0);
        Map<String, DcInfo> dcInfos = new ConcurrentHashMap<>();
        metaServers.forEach((dc, metaServerInfo) -> {
            dcInfos.put(dc, new DcInfo(metaServerInfo.getAddess()));
        });
        metaServer.start("http://127.0.0.1:" + console_port, "127.0.0.1:" + zkPort, dcInfos);
    }


    @Test
    public void startJQMetaServer() throws Exception {
        startMetaServer(JQ);
        Thread.currentThread().join();
    }
    
    @Test 
    public void startJQChecker2() throws Exception {
        startChecker2(JQ);
        Thread.currentThread().join();
    }
    
    @Test 
    public void StartOY() throws Exception {
        
    }
    
    @Test 
    public void OtherConsole() throws Exception {
//        startConsole2(OY, null);
        startChecker2(FRA);
        Thread.currentThread().join();
    }
    
    @Test 
    public void OtherMeta() throws Exception {
//        startMetaServer2(OY);
        startMetaServer2(FRA);
        Thread.currentThread().join();
    }
    
    @Test
    public void test() throws InterruptedException {
        RouteModel model = new RouteModel();
        model.setId(2).setSrcProxyIds("2").setTag("META").setDstProxyIds("1,5").setSrcDcName("jq").setDstDcName("fra").setActive(true);
        RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(3, 100);
        restTemplate.delete("http://127.0.0.1:8080/api/route", model);

    }


}
