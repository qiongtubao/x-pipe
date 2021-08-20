package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;

public class Utils {
    static int getPort(String uri) {
        return getEndpoint(uri).getPort();
    }
    
    static Endpoint getEndpoint(String uri) {
        String[] parts = uri.split(":");
        if (parts.length != 2) {
            throw new IllegalStateException("address wrong:" + uri);
        }
        int zkPort = Integer.parseInt(parts[1]);
        return new DefaultEndPoint(parts[0], zkPort);
    }
}
