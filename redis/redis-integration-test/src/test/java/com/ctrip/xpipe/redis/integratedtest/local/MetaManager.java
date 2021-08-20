package com.ctrip.xpipe.redis.integratedtest.local;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class MetaManager implements Lifecycle {
    XpipeMeta xpipeMeta;
    String configFile;
    public MetaManager(String configFile) {
       this.configFile = configFile;
    }

    @Override
    public void dispose() throws Exception {
        
    }
    protected XpipeMeta loadXpipeMeta(String configFile) throws IOException, SAXException {
        if (configFile == null) {
            return null;
        }
        //getClass 为了获取当前class的路径  找到相对路径
        InputStream ins = FileUtils.getFileInputStream(configFile, getClass());
        return DefaultSaxParser.parse(ins);
    }
    @Override
    public void initialize() throws Exception {
        xpipeMeta = loadXpipeMeta(configFile);
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

    public XpipeMeta getXpipeMeta() {
        return xpipeMeta;
    }

    @Test
    public void testLoadXpipeMeta() {
        MetaManager metaManager = new MetaManager("local/local.xml");
        XpipeMeta meta = metaManager.getXpipeMeta();
        
    }
    
}
