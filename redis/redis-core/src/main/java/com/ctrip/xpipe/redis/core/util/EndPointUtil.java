package com.ctrip.xpipe.redis.core.util;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;

public class EndPointUtil {
    static ProxyConnectProtocol getProxyProtocol(String host, int port, RouteMeta route) {
        String uri = String.format("%s://%s:%d", ProxyEndpoint.PROXY_SCHEME.TCP, host, port);
        String protocol = String.format("%s %s %s %s;", ProxyConnectProtocol.KEY_WORD, PROXY_OPTION.ROUTE, route.getRouteInfo(), uri);
        return new DefaultProxyConnectProtocolParser().read(protocol);
    }

    public static Endpoint create(RedisMeta redisMeta, RouteMeta routeMeta) {
        if(routeMeta != null) {
            return new ProxyEnabledEndpoint(redisMeta.getIp(), redisMeta.getPort(), getProxyProtocol(redisMeta.getIp(), redisMeta.getPort(), routeMeta));
        } else {
            return new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort());
        }
    }
}
