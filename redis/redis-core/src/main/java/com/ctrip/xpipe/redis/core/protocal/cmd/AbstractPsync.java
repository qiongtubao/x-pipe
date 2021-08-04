package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser.BulkStringParserListener;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         2016年3月24日 下午2:24:38
 */
public abstract class AbstractPsync extends AbstractRedisCommand<Object> implements Psync, BulkStringParserListener {
	//是否继续解析命令
	private boolean saveCommands;
	//rdb解析器
	private BulkStringParser rdbReader;
	//psync 发送前使用的replIdRequest
	private String replIdRequest;
	//psync 发送前使用的offset
	private long offsetRequest;
	//接收到的replid
	protected String replId;
	//接收到的offset
	protected long masterRdbOffset;
	/**
	 *
	 */
	protected List<PsyncObserver> observers = new LinkedList<PsyncObserver>();

	protected PSYNC_STATE psyncState = PSYNC_STATE.PSYNC_COMMAND_WAITING_REPONSE;

	public AbstractPsync(String host, int port, boolean saveCommands, ScheduledExecutorService scheduled) {
		super(host, port, scheduled);
		this.saveCommands = saveCommands;
	}

	public AbstractPsync(SimpleObjectPool<NettyClient> clientPool, boolean saveCommands,
			ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
		this.saveCommands = saveCommands;
	}

	@Override
	public String getName() {
		return "psync";
	}
	
	@Override
	protected void doExecute() throws CommandExecutionException {
		super.doExecute();
		addFutureListener();
		
	}

	//public for unit test
	public void addFutureListener() {
		future().addListener(new CommandFutureListener<Object>() {
			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				if(!commandFuture.isSuccess()){
					failPsync(commandFuture.cause());
				}
			}
		});
	}

	protected void failPsync(Throwable throwable) {
		if(psyncState == PSYNC_STATE.READING_RDB){
			failReadRdb(throwable);
		}
	}

	protected abstract void failReadRdb(Throwable throwable);

	@Override
	public ByteBuf getRequest() {

		Pair<String, Long> requestInfo = getRequestMasterInfo();

		replIdRequest = requestInfo.getKey();
		offsetRequest = requestInfo.getValue();

		if (replIdRequest == null) {
			replIdRequest = "?";
			offsetRequest = -1;
		}
		RequestStringParser requestString = new RequestStringParser(getName(), replIdRequest,
				String.valueOf(offsetRequest));
		if (getLogger().isDebugEnabled()) {
			getLogger().debug("[doRequest]{}, {}", this, StringUtil.join(" ", requestString.getPayload()));
		}
		//转换成psync命令
		return requestString.format();
	}

	protected abstract Pair<String, Long> getRequestMasterInfo();

	/**
	 *
	 * @param observer
	 *
	 * 		AbstractRedisMasterReplication -> LeakyBucketBasedMasterReplicationListener
	 *
	 * 		DefaultRedisMasterReplication
	 * 		redisKeeperServer
	 *
	 * 		RdbonlyRedisMasterReplication
	 */
	@Override
	public void addPsyncObserver(PsyncObserver observer) {
		this.observers.add(observer);
	}
	
	

	@Override
	protected Object doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {
		while(true) {
			switch (psyncState) {

				case PSYNC_COMMAND_WAITING_REPONSE:
					//解析出字符串
					/**
					 *  发送psync命令后返回结果
					 *  1.增量同步
					 *  	+CONTINUE
					 *  	+CONTINUE <replid>  (主从切换 replid变更  但是还是增量时返回会带replidId)
					 *  2.全量同步
					 *  	+FULLRESYNC <replid> <offsize>\r\n
					 *
					 *  	由于通过命令解析后 返回结果是去掉+的字符串
					 */
					Object response = super.doReceiveResponse(channel, byteBuf);
					if (response == null) {
						return null;
					}
					handleRedisResponse(channel, (String) response);
					break;

				case READING_RDB:

					if (rdbReader == null) {
						getLogger().info("[doReceiveResponse][createRdbReader]{}", ChannelUtil.getDesc(channel));
						rdbReader = createRdbReader();
						/**
						 *  主要是等到解析出协议（EOF还是RDBSize）触发onEofType事件
						 *  	1。AbstractReplicationStorePsync 类需要把写入数据写到文件,
						 *  	而onEofType事件会触发创建rdbStore 主要原因是需要知道rdb大小
						 */
						rdbReader.setBulkStringParserListener(this);
					}

					RedisClientProtocol<InOutPayload> payload = rdbReader.read(byteBuf);
					if (payload != null) {
						psyncState = PSYNC_STATE.READING_COMMANDS;
						if (!saveCommands) {
							/**
							 *  不继续解析命令的话 就返回成功
							 */
							future().setSuccess();
						}
						endReadRdb();
						break;
					} else {
						break;
					}
				case READING_COMMANDS:
					if (saveCommands) {
						try {
							appendCommands(byteBuf);
						} catch (IOException e) {
							getLogger().error("[doHandleResponse][write commands error]" + this, e);
						}
					}
					break;
				default:
					throw new IllegalStateException("unknown state:" + psyncState);
			}

			return null;
		}
	}

	protected void handleRedisResponse(Channel channel, String psync) throws IOException {

		if (getLogger().isInfoEnabled()) {
			getLogger().info("[handleRedisResponse]{}, {}, {}", ChannelUtil.getDesc(channel), this, psync);
		}
		//分割字符串
		String[] split = splitSpace(psync);
		if (split.length == 0) {
			throw new RedisRuntimeException("wrong reply:" + psync);
		}
		//全量同步
		if (split[0].equalsIgnoreCase(FULL_SYNC)) {
			if (split.length != 3) {
				throw new RedisRuntimeException("unknown reply:" + psync);
			}
			//replid
			replId = split[1];
			//offset
			masterRdbOffset = Long.parseLong(split[2]);
			getLogger().debug("[readRedisResponse]{}, {}, {}, {}", ChannelUtil.getDesc(channel), this, replId,
					masterRdbOffset);
			//进入下一步接收rdb
			psyncState = PSYNC_STATE.READING_RDB;

			doOnFullSync();
		} else if (split[0].equalsIgnoreCase(PARTIAL_SYNC)) {
			//增量同步
			//进入下一步解析命令
			psyncState = PSYNC_STATE.READING_COMMANDS;
			
			String newReplId = null;
			if(split.length >= 2 && split[1].length() == RedisProtocol.RUN_ID_LENGTH){
				//更新replid
				newReplId = split[1];
			}
			doOnContinue(newReplId);
		} else {
			throw new RedisRuntimeException("unknown reply:" + psync);
		}
	}

	protected void endReadRdb() {

		getLogger().info("[endReadRdb]");
		for (PsyncObserver observer : observers) {
			try {
				observer.endWriteRdb();
			} catch (Throwable th) {
				getLogger().error("[endReadRdb]" + this, th);
			}
		}
	}

	protected abstract void appendCommands(ByteBuf byteBuf) throws IOException;

	protected abstract BulkStringParser createRdbReader();

	protected void doOnFullSync() throws IOException {
		getLogger().debug("[doOnFullSync]");
		notifyFullSync();
	}

	private void notifyFullSync() {
		getLogger().debug("[notifyFullSync]");
		for (PsyncObserver observer : observers) {
			observer.onFullSync(masterRdbOffset);
		}
	}
	
	protected void doOnContinue(String newReplId) throws IOException{
		getLogger().debug("[doOnContinue]{}",newReplId);
		notifyContinue(newReplId);
	}

	private void notifyContinue(String newReplId) {

		for (PsyncObserver observer : observers) {
			observer.onContinue(replIdRequest, newReplId);
		}
	}

	@Override
	public void onEofType(EofType eofType) {
		beginReadRdb(eofType);
	}

	/**
	 *
	 * @param eofType
	 * 		DefaultRedisKeeperServer -> RDBStore写入"1"
	 */
	protected void beginReadRdb(EofType eofType) {

		getLogger().info("[beginReadRdb]{}, eof:{}", this, eofType);

		for (PsyncObserver observer : observers) {
			try {
				observer.beginWriteRdb(eofType, replId, masterRdbOffset);
			} catch (Throwable th) {
				getLogger().error("[beginReadRdb]" + this, th);
			}
		}
	}

	/**
	 *
	 * 		 DefaultRedisKeeperServer  关闭下游所有slave
	 *
	 */
	protected void notifyReFullSync() {

		for (PsyncObserver observer : observers) {
			observer.reFullSync();
		}
	}

	@Override
	protected Object format(Object payload) {
		return payload;
	}

	@Override
	public int getCommandTimeoutMilli() {
		return 0;
	}

	@Override
	protected void doReset() {
		throw new UnsupportedOperationException("not supported");
	}
}
