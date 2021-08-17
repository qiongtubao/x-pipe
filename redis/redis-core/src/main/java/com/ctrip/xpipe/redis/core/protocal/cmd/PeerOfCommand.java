package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.route.RouteOptionParser;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyConnectProtocol;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.ArrayUtils;

import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class PeerOfCommand extends AbstractRedisCommand {

    protected long gid;
    //修改传入参数变成(ip, port) -> endpoint
    protected Endpoint endpoint;

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, Endpoint endpoint, ScheduledExecutorService scheduled){
        this(clientPool, gid, endpoint, "", scheduled);
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, Endpoint endpoint, String param, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
        this.endpoint = endpoint;
    }

    @Override
    public String getName() {
        return "peerof";
    }

    static final String TEMP_PROXY_TYPE = "proxy-type";
    static final String TEMP_PROXY_SERVERS = "proxy-servers";
    static final String TEMP_PROXY_PARAMS = "proxy-params";

    @Override
    public ByteBuf getRequest() {

        RequestStringParser requestString = null;
        if(endpoint == null){
            requestString = new RequestStringParser(getName(), String.valueOf(gid), "no", "one");
        }else{
            String[] params = ArrayUtils.addAll(null, getName(), String.valueOf(gid), endpoint.getHost(), String.valueOf(endpoint.getPort()));
            if(endpoint instanceof ProxyEnabled) {
               ProxyConnectProtocol protocol = ((ProxyEnabled)endpoint).getProxyProtocol();
               if(protocol instanceof DefaultProxyConnectProtocol) {
                   RouteOptionParser parser = (RouteOptionParser)((DefaultProxyConnectProtocol) protocol).getParser().getProxyOptionParser(PROXY_OPTION.ROUTE);
                   if( parser != null) {
                       String servers = parser.getNextEndpoints().stream().map(endpoint -> {
                           return endpoint.getHost() + ":" + endpoint.getPort();
                       }).collect(Collectors.joining(","));
                       params = ArrayUtils.addAll(params, TEMP_PROXY_TYPE, "XPIPE-PROXY", TEMP_PROXY_SERVERS, servers);
                       String proxyParams = new RouteOptionParser().read(parser.output()).getContent();
                       if(proxyParams != null) {
                           params = ArrayUtils.addAll(params, TEMP_PROXY_PARAMS, "\"" + proxyParams + "\"");
                       }
                   }
               }
            }
            requestString = new RequestStringParser(params);
        }
        return requestString.format();
    }

    @Override
    public String toString() {

        String target = getClientPool() == null? "null" : getClientPool().desc();

        if(endpoint == null){
            return String.format("%s: %s %d no one", target, getName(), gid);
        }else{
            return String.format("%s: %s %d %s %d", target, getName(), gid, endpoint.getHost(), endpoint.getPort());
        }
    }

    @Override
    protected String format(Object payload) {
        return payloadToString(payload);
    }

}
