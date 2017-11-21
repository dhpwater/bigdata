package com.ns.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 精典Reactor模式
 * 从代码中可以看到，多个Channel可以注册到同一个Selector对象上，实现了一个线程同时监控多个请求状态（Channel）。
 * 同时注册时需要指定它所关注的事件，例如上示代码中socketServerChannel对象只注册了OP_ACCEPT事件，
 * 而socketChannel对象只注册了OP_READ事件。
 * selector.select()是阻塞的，当有至少一个通道可用时该方法返回可用通道个数。同时该方法只捕获Channel注册时指定的所关注的事件。
 * 
 * @author ryan
 *
 */

public class NIOServer {

	private static Logger logger = LoggerFactory.getLogger(NIOServer.class);

	public static void main(String[] args) throws IOException {
		Selector selector = Selector.open();
		ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.configureBlocking(false);
		serverSocketChannel.bind(new InetSocketAddress(8888));
		serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
		while (selector.select() > 0) {
			Set<SelectionKey> keys = selector.selectedKeys();
			Iterator<SelectionKey> iterator = keys.iterator();
			while (iterator.hasNext()) {
				SelectionKey key = iterator.next();
				iterator.remove();
				if (key.isAcceptable()) {
					ServerSocketChannel acceptServerSocketChannel = (ServerSocketChannel) key.channel();
					SocketChannel socketChannel = acceptServerSocketChannel.accept();
					socketChannel.configureBlocking(false);
					logger.info("Accept request from {}", socketChannel.getRemoteAddress());
					socketChannel.register(selector, SelectionKey.OP_READ);
				} else if (key.isReadable()) {
					SocketChannel socketChannel = (SocketChannel) key.channel();
					ByteBuffer buffer = ByteBuffer.allocate(1024);
					int count = socketChannel.read(buffer);
					if (count <= 0) {
						socketChannel.close();
						key.cancel();
						logger.info("Received invalide data, close the connection");
						continue;
					}
					System.out.println(new String(buffer.array()));
					logger.info("Received message {}", new String(buffer.array()));
				}
				keys.remove(key);
			}
		}
	}
}
