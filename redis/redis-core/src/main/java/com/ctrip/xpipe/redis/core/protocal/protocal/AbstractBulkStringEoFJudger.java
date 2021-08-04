package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * Dec 22, 2016
 */
public abstract class AbstractBulkStringEoFJudger implements BulkStringEofJudger{

	protected AtomicLong realLen = new AtomicLong();

	@Override
	public JudgeResult end(ByteBuf byteBuf) {
		//先增加读取长度
		realLen.addAndGet(byteBuf.readableBytes());
		return doEnd(byteBuf);
	}
	protected abstract JudgeResult doEnd(ByteBuf byteBuf);


	public static class BulkStringLengthEofJudger extends AbstractBulkStringEoFJudger{
		
		private final long expectedLen;
		
		public BulkStringLengthEofJudger(long expectedLen) {
			if(expectedLen < 0){
				throw new RedisRuntimeException("expectedLen < 0:" + expectedLen);
			}
			this.expectedLen = expectedLen;
		}

		@Override
		public JudgeResult doEnd(ByteBuf byteBuf) {
			int readable = byteBuf.readableBytes();
			long currentLen = realLen.get();
			if(currentLen >= expectedLen){
				//当前长度 >= 结束长度
				//应该读取的长度 - (当前长度 - 读取的长度)
				int read = (int) (expectedLen - (currentLen - readable));
				if(read < 0){
					throw new IllegalStateException("readlen < 0:" + read);
				}
				return new JudgeResult(true, read);
			}else{
				return new JudgeResult(false, readable);
			}
		}

		@Override
		public long expectedLength() {
			return expectedLen;
		}
		
		@Override
		public String toString() {
			return String.format("expectedLen:%d, readLen:%d", expectedLen, realLen.get());
		}

		@Override
		public int truncate() {
			return 0;
		}

		@Override
		public EofType getEofType() {
			return new LenEofType(expectedLen);
		}
		
	}
	
	
	public static class BulkStringEofMarkJudger extends AbstractBulkStringEoFJudger{
		
		public static final int MARK_LENGTH = RedisClientProtocol.RUN_ID_LENGTH;
		
		private final byte[] eofmark;
		
		private volatile boolean alreadyFinished = false;
		
		private byte lastData[] = new byte[MARK_LENGTH];
		
		public BulkStringEofMarkJudger(byte[] eofmark) {
			this.eofmark = eofmark;
		}

		@Override
		public JudgeResult doEnd(ByteBuf raw) {
			
			if(alreadyFinished){
				throw new IllegalStateException("doEnd already ended:" + this);
			}
			/**
			 *  普通AbstractByteBuf  数据指针是一个 对象内记录的writeindex 和 readindex length等不同
			 *  slice() = slice(buf.readerIndex(), buf.readableBytes())
			 *  	public ByteBuf slice(int index, int length) {
			 *         ensureAccessible();
			 *         return new UnpooledSlicedByteBuf(this, index, length);
			 *     }
			 *
			 */
			ByteBuf used = raw.slice();
			//byteBuf长度
			int readable = used.readableBytes();
			//长度大于40
			if(readable > MARK_LENGTH){
				//设置最后40个字符
				int writeIndex = used.writerIndex();
				used.readerIndex(writeIndex - MARK_LENGTH);
			}
			//可读长度值应该是 <= 40
			int dataLen = used.readableBytes();
			//需要截取上次数据的长度
			int remLen = MARK_LENGTH - dataLen;
			/**
			 *  为了保留上一次最后的数据 进行拷贝
			 *  lastData 保存40个字符
			 *  1.不是最后一次收到的数据
			 *  	System.arraycopy(lastData, 40, lastData, 0, 0);
			 *  	没进行拷贝
			 *  2. 如果是最后一次（假设最后一次发送来的是10个字符)
			 *  	System.arraycopy(lastData, 10, lastData, 0, 30);
			 *  	截取后30个字符移到前面30位
			 */
			System.arraycopy(lastData, dataLen, lastData, 0, remLen);
			//读取数据 写入lastData  从remLen 开始
			/**
			 *  1.不是最后一次收到数据
			 *  	由于之前设置读取位置在最后40位
			 *  	used.readBytes(lastData, 0, 40);
			 *  	拷贝了40字符保存到lastData
			 *  2.如果是最后一次(10个字符)
			 *		used.readBytes(lastData, 30, 10);
			 *		读取数据写入到lastData（从index 30 开始写入10个字符)
			 */
			used.readBytes(lastData, remLen, dataLen);
			
			boolean ends = Arrays.equals(eofmark, lastData);
			//对比byteArray值是否一致
			if(ends){
				alreadyFinished = true;
			}
			return new JudgeResult(ends, readable);
		}

		public byte[] getLastData() {
			return lastData;
		}

		@Override
		public int truncate() {
			return MARK_LENGTH;
		}

		@Override
		public long expectedLength() {
			return -1;
		}
		
		@Override
		public String toString() {
			return String.format("eofmark:%s, realLen:%d", new String(eofmark), realLen.get());
		}

		@Override
		public EofType getEofType() {
			return new EofMarkType(new String(eofmark));
		}

	}
}
