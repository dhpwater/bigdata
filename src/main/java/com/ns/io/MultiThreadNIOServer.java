package com.ns.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从上示代码中可以看到，注册完SocketChannel的OP_READ事件后，
 * 可以对相应的SelectionKey attach一个对象（本例中attach了一个Processor对象，该对象处理读请求），
 * 并且在获取到可读事件后，可以取出该对象。
 * 注：attach对象及取出该对象是NIO提供的一种操作，但该操作并非Reactor模式的必要操作，本文使用它，只是为了方便演示NIO的接口。
    
 * 具体的读请求处理在如下所示的Processor类中。
 * 该类中设置了一个静态的线程池处理所有请求。
 * 而process方法并不直接处理I/O请求，而是把该I/O操作提交给上述线程池去处理，
 * 这样就充分利用了多线程的优势，同时将对新连接的处理和读/写操作的处理放在了不同的线程中，读/写操作不再阻塞对新连接请求的处理。
 * 
 * 
 * @author ryan
 *
 */

public class MultiThreadNIOServer {

	private static Logger logger = LoggerFactory.getLogger(MultiThreadNIOServer.class);

	public static void main(String[] args) throws IOException {

		Selector selector = Selector.open();
		ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.configureBlocking(false);
		serverSocketChannel.bind(new InetSocketAddress(1234));
		serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
		while (true) {
			if (selector.selectNow() < 0) {
				continue;
			}
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
					SelectionKey readKey = socketChannel.register(selector, SelectionKey.OP_READ);
					readKey.attach(new Processor());
				} else if (key.isReadable()) {
					Processor processor = (Processor) key.attachment();
					processor.process(key);
				}
			}
		}
	}

}
