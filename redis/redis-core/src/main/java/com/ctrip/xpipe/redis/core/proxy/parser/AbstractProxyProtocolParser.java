package com.ctrip.xpipe.redis.core.proxy.parser;

import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.exception.ProxyProtocolException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.exception.ProxyProtocolParseException;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.util.List;

import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.LINE_SPLITTER;
import static com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser.WHITE_SPACE;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public abstract class AbstractProxyProtocolParser<V extends ProxyProtocol> implements ProxyProtocolParser {

    private SimpleStringParser simpleStringParser = new SimpleStringParser();

    private List<ProxyOptionParser> parsers = Lists.newArrayList();

    private static final char IMPORTANT_OPTION_SIGN = '#';

    @Override
    public ProxyOptionParser getProxyOptionParser(PROXY_OPTION proxyOption) {
        for(ProxyOptionParser parser : parsers) {
            if(parser.option() == proxyOption) {
                return parser;
            }
        }
        return null;
    }

    @Override
    public ByteBuf format() {
        StringBuilder proxyProtocol = new StringBuilder(ProxyProtocol.KEY_WORD).append(WHITE_SPACE);
        for(ProxyOptionParser parser : parsers) {
            proxyProtocol.append(parser.output());
            if(parser.isImportant()) {
                proxyProtocol.append(IMPORTANT_OPTION_SIGN);
            }
            proxyProtocol.append(";");
        }
        return new SimpleStringParser(proxyProtocol.toString()).format();
    }

    @Override
    public V read(String protocol) {
        //以ROPXY 开头的协议才行
        if(!protocol.toLowerCase().startsWith(ProxyProtocol.KEY_WORD.toLowerCase())) {
            throw new ProxyProtocolException("proxy protocol format error: " + protocol);
        }
        //去除PROXY
        String options = removeKeyWord(protocol);
        //分割   \\s*;\\s*
        String[] allOption = options.split(LINE_SPLITTER);
        //
        boolean optionImportant = false;
        for(String option : allOption) {
            //判断最后一个字符是否为# 
            if((optionImportant = isOptionImportant(option))) {
                option = option.substring(0, option.length() - 1);
            }
            //解析出Parser
            ProxyOptionParser optionParser = PROXY_OPTION.getOptionParser(option.trim());
            if(optionImportant && optionParser.option().equals(PROXY_OPTION.UNKOWN)) {
                throw new ProxyProtocolParseException(String.format("UNKNOWN IMPORTANT PROXY PROTOCOL OPTION: %s", option));
            }
            addProxyParser(optionParser);
        }
        validate(parsers);
        return newProxyProtocol(protocol);
    }

    @Override
    public V read(ByteBuf byteBuf) {
        RedisClientProtocol<String> redisClientProtocol = simpleStringParser.read(byteBuf);
        if(redisClientProtocol == null) {
            return null;
        }
        return read(redisClientProtocol.getPayload());
    }

    private boolean isOptionImportant(String option) {
        return option.charAt(option.length() - 1) == IMPORTANT_OPTION_SIGN;
    }

    protected String removeKeyWord(String protocol) {
        return protocol.substring(ProxyProtocol.KEY_WORD.length());
    }

    private void addProxyParser(ProxyOptionParser parser) {
        parsers.add(parser);
    }

    protected List<ProxyOptionParser> getParsers() {
        return parsers;
    }

    protected abstract V newProxyProtocol(String protocol);

    protected void validate(List<ProxyOptionParser> parsers) {}
}
