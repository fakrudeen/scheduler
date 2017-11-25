package com.fakrudeen.scheduler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Master web service of the scheduling system.
 * Author: Fakrudeen Ali Ahmed
 * Date: 23 Nov 2017
 */
public class WebService {
    private static final Logger LOGGER = Logger.getLogger(WebService.class.getName());
    public static final String SCHEDULER_CONTEXT = "/scheduler";
    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String CONTENT_TYPE_HEADER_VALUE = "Content-Type: text/plain; charset=utf-8";
    public static final String EMPTY_RETURN_VALUE = "{}";
    public static final String SCHEDULE_METHOD = "schedule";

    public static void main(String[] args) throws IOException {
        if(1 != args.length) {
            System.out.println("Scheduler needs exactly one argument for master port.");
            return;
        }
        int port = Integer.parseInt(args[0]);
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext(SCHEDULER_CONTEXT, new HttpHandler() {
            Scheduler scheduler = new Scheduler();
            public void handle(HttpExchange exchange) throws IOException {
                //request
                RequestHandler requestHandler = new RequestHandler(exchange).handle();
                String method = requestHandler.getMethod();
                String[] requestParts = requestHandler.getRequestParts();

                //scheduler call
                Object returnValue = callScheduler(scheduler, method, requestParts[3], requestParts.length > 4 ? requestParts[4] : null);
                String output = (null == returnValue) ? EMPTY_RETURN_VALUE : returnValue.toString();

                //response
                handleHeaders(exchange, output.length(), isSuccess(method, returnValue));
                handleOutput(exchange, output);
            }
        });
        server.start();
    }

    /**
     * Delegates calls to scheduler
     * @param scheduler scheduler
     * @param method method to call
     * @param worker worker sending in the request
     * @param task task if any.
     * @return output from scheduler
     */
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
            case SCHEDULE_METHOD:
                return scheduler.schedule(worker);
        }
        return null;
    }

    /**
     * Handles output to workers
     * @param exchange http exchange object for response
     * @param output output to write
     */
    private static void handleOutput(HttpExchange exchange, String output) throws IOException {
        OutputStream outputStream = exchange.getResponseBody();
        try(OutputStreamWriter writer = new OutputStreamWriter(outputStream)) {
            writer.write(output);
        }
    }

    /**
     * Handles output headers
     * @param exchange exchange object for headers
     * @param length output length
     * @param isSuccess whether request processing is successful
     */
    private static void handleHeaders(HttpExchange exchange, int length, boolean isSuccess) throws IOException {
        if(isSuccess) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, length);
        } else {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_CONFLICT, 0);
        }
        exchange.getResponseHeaders().add(CONTENT_TYPE_HEADER, CONTENT_TYPE_HEADER_VALUE);
    }

    /**
     * Whether the request is successful
     * @param method method called
     * @param returnValue return value
     */
    private static boolean isSuccess(String method, Object returnValue) {
        return !(method.equalsIgnoreCase(SCHEDULE_METHOD) && null == returnValue);
    }

    /**
     * Request handling object - required due to mulitple return values.
     */
    private static class RequestHandler {
        private HttpExchange exchange;
        private String[] requestParts;
        private String method;

        public RequestHandler(HttpExchange exchange) {
            this.exchange = exchange;
        }

        public String[] getRequestParts() {
            return requestParts;
        }

        public String getMethod() {
            return method;
        }

        public RequestHandler handle() {
            URI requestURI = exchange.getRequestURI(); // /scheduler/join/1
            requestParts = requestURI.getPath().split("/");
            LOGGER.log(Level.INFO, requestURI.toString());
            method = requestParts[2];
            return this;
        }
    }
}
