package com.fakrudeen.scheduler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.URI;

public class WebService {
    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/scheduler", new HttpHandler() {
            Scheduler scheduler = new Scheduler();
            public void handle(HttpExchange exchange) throws IOException {
                URI requestURI = exchange.getRequestURI(); // /scheduler/join/1
                String[] requestParts = requestURI.getPath().split("/");

                System.out.println(requestURI);
                Object returnValue = callScheduler(scheduler, requestParts[2], requestParts[3], requestParts.length > 4 ? requestParts[4] : null);
                String output = null == returnValue ? "Hello World!" : returnValue.toString();
                exchange.sendResponseHeaders(200, output.length());
                exchange.getResponseHeaders().add("Content-Type", "Content-Type: text/plain; charset=utf-8");
                OutputStream outputStream = exchange.getResponseBody();
                try(OutputStreamWriter writer = new OutputStreamWriter(outputStream)) {
                    writer.write(output);
                }
            }
        });
        server.start();
    }

    public static Object callScheduler(Scheduler scheduler, String method, String worker, String task) {
        switch(method) {
            case "join":
                scheduler.join(worker);
                break;
            case "leave":
                scheduler.leave(worker);
                break;
            case "heartbeat":
                scheduler.heartBeat(worker);
                break;
            case "finished":
                scheduler.finished(worker, task);
                break;
            case "schedule":
                return scheduler.schedule(worker);
        }
        return null;
    }

}
