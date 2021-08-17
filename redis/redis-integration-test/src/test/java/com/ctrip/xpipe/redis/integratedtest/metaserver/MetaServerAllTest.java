package com.ctrip.xpipe.redis.integratedtest.metaserver;


import com.ctrip.xpipe.redis.integratedtest.metaserver.scenes.RouteChangeTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        RouteChangeTest.class,
//        CrdtPeerMasterChange.class,
//        CrdtCurrentMasterChange.class,
//        AddCluster.class,
//        RemoveCluster.class,
//        ClusterTypeToCrdt.class
})
public class MetaServerAllTest {

}
