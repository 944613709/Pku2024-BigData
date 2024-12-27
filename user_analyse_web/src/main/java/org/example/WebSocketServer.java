package org.example;

import com.alibaba.fastjson.JSON;
import org.springframework.stereotype.Component;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Map;

/**
 * WebSocket服务处理类
 * @ServerEndpoint 该注解将类定义成一个WebSocket服务器端，并指定前端的请求地址
 * @OnOpen 表示客户端与服务器连接成功的时候被调用
 * @OnClose 表示客户端与服务器断开连接的时候被调用
 * @OnMessage 表示服务器接收到客户端发送的消息后被调用
 */
@Component
@ServerEndpoint("/websocket")
public class WebSocketServer {

    JdbcUtil jdbcUtil = new JdbcUtil();
    /**
     * 服务器接收到客户端发送的消息后，自动调用该方法
     * @param message 客户端发送的消息
     * @param session 服务器与客户端的session会话
     */
    @OnMessage
    public void onMessage(String message, Session session) throws IOException, InterruptedException {
        while(true){
            //调用业务层的方法，查询MySQL数据库的计算结果
            Map<String, Object> map = jdbcUtil.queryResultData();
            //向客户端发送JSON格式的消息
            session.getBasicRemote().sendText(JSON.toJSONString(map));
            //每隔1秒查询一次最新结果
            Thread.sleep(1000);
            map.clear();
        }
    }

    /**
     * 客户端与服务器连接成功，自动调用该方法
     */
    @OnOpen
    public void onOpen () {
        System.out.println("Client connected");
    }
    /**
     * 客户端与服务器断开连接，自动调用该方法
     */
    @OnClose
    public void onClose () {
        System.out.println("Connection closed");
    }
}
