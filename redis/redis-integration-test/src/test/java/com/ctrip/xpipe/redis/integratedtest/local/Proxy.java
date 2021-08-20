package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.redis.integratedtest.metaserver.proxy.LocalProxyConfig;
import com.ctrip.xpipe.redis.integratedtest.metaserver.proxy.LocalResourceManager;
import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.monitor.DefaultTunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultPingStatsManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;

public class Proxy implements Lifecycle {
    ProxyInfo info;
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

    }

    @Override
    public int getOrder() {
        return 0;
    }
    DefaultProxyServer server;
    
    public Proxy(ProxyInfo proxyInfo) {
        this.info = proxyInfo;
    }
    
    @Override
    public void start() throws Exception {
        LocalProxyConfig proxyConfig = new LocalProxyConfig();
        proxyConfig.setFrontendTcpPort(info.tcp_port).setFrontendTlsPort(info.tls_port);
        ResourceManager resourceManager = new LocalResourceManager(proxyConfig);
        TunnelMonitorManager tunnelMonitorManager = new DefaultTunnelMonitorManager(resourceManager);
        TunnelManager tunnelManager = new DefaultTunnelManager()
                .setConfig(proxyConfig)
                .setProxyResourceManager(resourceManager)
                .setTunnelMonitorManager(tunnelMonitorManager);
        DefaultProxyServer server = new DefaultProxyServer().setConfig(proxyConfig);
        server.setTunnelManager(tunnelManager);
        server.setResourceManager(resourceManager);
        server.setPingStatsManager(new DefaultPingStatsManager());
        server.start();
        this.server = server;
// subProcessCmds.add(server);
    }

    static class ProxyInfo {
        public int tcp_port;
        public int tls_port;
        public ProxyInfo(int tcp_port, int tls_port) {
            this.tcp_port = tcp_port;
            this.tls_port = tls_port;
        }
    }
    
}
