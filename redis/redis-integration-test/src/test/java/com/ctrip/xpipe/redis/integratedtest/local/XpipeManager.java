package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;

public class XpipeManager implements Lifecycle {
    MetaManager metaManager;

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

    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public int getOrder() {
        return 0;
    }
}
