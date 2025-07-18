package com.redis;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import com.redis.configAndUtils.Config;
import com.redis.configAndUtils.RdbUtils;
import com.redis.configAndUtils.Roles;
import com.redis.configAndUtils.Utils;
import com.redis.serverProfile.MasterProfile;
import com.redis.serverProfile.SlaveProfile;

public class Main {

    public static void main(String[] args) throws UnknownHostException, IOException {

        Utils.readConfiguration(args);

        if (!Config.dir.isEmpty() && !Config.dbfilename.isEmpty()) {
            RdbUtils.processRdbFile();
        }

        // doing a asynchronous function call
        if (Config.role.equals(Roles.SLAVE) && Config.isHandshakeComplete == false) {
            new Thread((new Runnable() {
                public void run() {
                    try {
                        SlaveProfile.handshake();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            })).start();
        }

        Config.printConfig();

        System.out.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        try {
            serverSocket = new ServerSocket(Config.port);
            serverSocket.setReuseAddress(true);

            while (true) {

                clientSocket = serverSocket.accept();

                if (Config.role.equals(Roles.MASTER)) {
                    MasterProfile requestHandler = new MasterProfile(clientSocket);
                    Thread.startVirtualThread(requestHandler);

                } else {
                    SlaveProfile slaveRequestHandler = new SlaveProfile(clientSocket);
                    Thread.startVirtualThread(slaveRequestHandler);

                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
