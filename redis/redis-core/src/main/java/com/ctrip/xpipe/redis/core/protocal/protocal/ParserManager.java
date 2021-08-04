package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Sep 14, 2016
 */
public class ParserManager {
	
	public static List<RedisClientProtocol<?>> parsers;
	protected static final Logger logger = LoggerFactory.getLogger(ParserManager.class);
	
	static {
		
		parsers = new ArrayList<>(10);
		parsers.add(new ArrayParser());
		parsers.add(new SimpleStringParser());
		parsers.add(new LongParser());
		parsers.add(new RedisErrorParser());
		parsers.add(new BulkStringParser(""));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> ByteBuf parse(T o){
		//通过反射 将参数解析成ByteBuf
		for(RedisClientProtocol parser : parsers){
			if(parser.supportes(o.getClass())){
				
				Constructor constructor = getConstructor(parser.getClass(), o.getClass());
				if(constructor == null){
					logger.warn("[getOptionParser][support argument, but can not find constructor]{},{}", parser, o);
					continue;
				}
				RedisClientProtocol curParser;
				try {
					curParser = (RedisClientProtocol) constructor.newInstance(o);
					return curParser.format();
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
						| InvocationTargetException e) {
					logger.error("[getOptionParser][invocate constructor error]" + parser + "," + o, e);
				}
			}
		}
		throw new IllegalStateException("unknown parser for o:" + o.getClass());
	}

	@SuppressWarnings("rawtypes")
	/**
	 *   返回class构造方法
	 */
	private static <T> Constructor<?> getConstructor(Class<T> clazz, Class argument) {
		//获得class构造方法
		for(Constructor<?>  constructor : clazz.getConstructors()){
			Class<?>[]paraTypes = constructor.getParameterTypes();
			//判断参数
			if(paraTypes.length != 1){
				continue;
			}
			//判断参数类型是否和预计参数类型相同
			if(paraTypes[0].isAssignableFrom(argument)){
				return constructor;
			}
		}
		return null;
	}
	

}
