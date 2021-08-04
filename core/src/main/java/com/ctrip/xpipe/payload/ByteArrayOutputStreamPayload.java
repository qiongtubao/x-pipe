package com.ctrip.xpipe.payload;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * 2016年4月24日 下午8:53:30
 */
public class ByteArrayOutputStreamPayload extends AbstractInOutPayload{

	private int INIT_SIZE = 2 << 10;
	private byte []data;
	private AtomicInteger pos  = new AtomicInteger(0);
	
	public ByteArrayOutputStreamPayload() {
		
	}

	public ByteArrayOutputStreamPayload(int INIT_SIZE) {
		//设置初始化值
		if(INIT_SIZE > 0) {
			this.INIT_SIZE = INIT_SIZE;
		}
	}

	public ByteArrayOutputStreamPayload(String message) {
		//初始化数据
		data = message.getBytes();
		pos.set(data.length);
	}

	@Override
	public void doStartInput() {
		 //初始化
		 data = new byte[INIT_SIZE];
		 pos.set(0);
	}

	@Override
	public int doIn(ByteBuf byteBuf) throws IOException {
		//写入数据大小
		int size = byteBuf.readableBytes();
		//尝试扩容
		makeSureSize(size);
		//从byteBuf 读取数据写到data内
		byteBuf.readBytes(data, pos.get(), size);
		//计算出写入量
		int read = size - byteBuf.readableBytes(); 
		//增加写入量
		pos.addAndGet(read);
		
		return read;
	}

	private void makeSureSize(int size) {
		//判断容量是否足够
		if(pos.get() + size > data.length){
			//创建新的容量  当前数据量*2 和 当前需要的容量大小做对比 （取最大值)
			byte []newData = new byte[ensureSize(size)];
			//拷贝数据
			System.arraycopy(data, 0, newData, 0, data.length);
			data = newData;
		}
	}

	private int ensureSize(int size) {
		//当前数据量*2 和 当前需要的容量大小做对比 （取最大值)
		int predictSize = data.length * 2, requireSize = size + pos.get();
		return predictSize > requireSize ? predictSize : requireSize;
	}

	@Override
	public long doOut(WritableByteChannel writableByteChannel) throws IOException {
		//把数据写入到WritableByteChannel  返回写入长度
		return writableByteChannel.write(ByteBuffer.wrap(data, 0, pos.get()));
	}

	
	public byte[] getBytes(){
		//获得长度
		int currentPos = pos.get();
		//创建等长的byte数组
		byte []dst = new byte[currentPos];
		//拷贝数据
		System.arraycopy(data, 0, dst, 0, currentPos);
		return dst;
	}
	
	@Override
	public String toString() {
		//返回字符串
		if(data != null){
			return new String(data, 0 , pos.get());
		}
		return super.toString();
	}

	@Override
	protected void doTruncate(int reduceLen) {
		//截断reduceLen 长度数据
		pos.addAndGet(-reduceLen);
	}
}
