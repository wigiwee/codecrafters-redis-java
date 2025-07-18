package com.redis.serverProfile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import com.redis.configAndUtils.Config;
import com.redis.configAndUtils.RdbUtils;
import com.redis.configAndUtils.Utils;

public class SlaveProfile implements Runnable {

    public Socket clientSocket;

    public static ConcurrentHashMap<String, String> keyValueHashMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Long> keyExpiryHashMap = new ConcurrentHashMap<>();

    public SlaveProfile(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));) {

            String reqLength;

            while ((reqLength = reader.readLine()) != null) {

                if (reqLength.startsWith("*")) {
                    int numArgs = Integer.parseInt(reqLength.substring(1));
                    String[] args = new String[numArgs];
                    for (int i = 0; i < numArgs; i++) {
                        String lineLength = reader.readLine();
                        if (!lineLength.startsWith("$")) {
                            writer.write("-ERROR: Invalid RESP format\r\n");
                            writer.flush();
                            continue;
                        }
                        int length = Integer.parseInt(lineLength.substring(1));
                        args[i] = reader.readLine();
                        if (args[i].length() != length) {
                            writer.write("-ERROR: Length mismatch\r\n");
                            writer.flush();
                            continue;
                        }
                    }
                    processCommand(writer, args, -1);

                } else {
                    writer.write("-ERROR: Unknown command or incorrect arguments\r\n");
                    writer.flush();
                }
            }

        } catch (IOException e) {
            System.out.println("Something went wrong while processing the command");
            System.out.println("Cannot read/write from the socket");
            e.printStackTrace();
        }
    }

    public static void processCommand(BufferedWriter writer, String[] args, int commandBytesLength) throws IOException {
        System.out.println("commandBytesLength: " + commandBytesLength);
        int numArgs = args.length;

        if (args[0].equalsIgnoreCase("ping")) {

            if (commandBytesLength == -1) {
                writer.write("+PONG\r\n");
                writer.flush();
            }

        } else if (args[0].equalsIgnoreCase("echo") && numArgs == 2) {

            String message = args[1];
            if (commandBytesLength == -1) {
                writer.write(Utils.bulkString(message));
                writer.flush();
            }

        } else if (args[0].equalsIgnoreCase("set") && numArgs >= 3) {
            keyValueHashMap.put(args[1], args[2]);
            if (numArgs > 3) {
                if (args[3].equalsIgnoreCase("px")) {
                    keyExpiryHashMap.put(args[1], System.currentTimeMillis() + Long.parseLong(args[4]));
                }
            } else if (numArgs == 3) {
                keyValueHashMap.put(args[1], args[2]);
            }
            if (commandBytesLength == -1) {
                writer.write("+OK\r\n");
                writer.flush();
            }
            // Utils.sendReplicaionCommands(args);

        } else if (args[0].equalsIgnoreCase("get") && numArgs == 2) {
            if (keyValueHashMap.containsKey(args[1])) {
                if (keyExpiryHashMap.containsKey(args[1])) {
                    if (System.currentTimeMillis() < keyExpiryHashMap.get(args[1])) {
                        if (commandBytesLength == -1) {
                            writer.write(Utils.bulkString(keyValueHashMap.get(args[1])));
                            writer.flush();
                        }
                    } else {
                        keyExpiryHashMap.remove(args[1]);
                        keyValueHashMap.remove(args[1]);
                        if (commandBytesLength == -1) {
                            writer.write(Config.NIL);
                            writer.flush();
                        }
                    }
                } else {
                    if (commandBytesLength == -1) {
                        writer.write(Utils.bulkString(keyValueHashMap.get(args[1])));
                        writer.flush();
                    }
                }
            } else if (RdbUtils.RDBkeyValueHashMap.containsKey(args[1])) {
                if (RdbUtils.RDBkeyExpiryHashMap.containsKey(args[1])) {
                    if (System.currentTimeMillis() < RdbUtils.RDBkeyExpiryHashMap.get(args[1])) {
                        if (commandBytesLength == -1) {
                            writer.write(Utils.bulkString(RdbUtils.RDBkeyValueHashMap.get(args[1])));
                            writer.flush();
                        }
                    } else {
                        if (commandBytesLength == -1) {
                            writer.write(Config.NIL);
                            writer.flush();
                        }
                    }
                } else {
                    if (commandBytesLength == -1) {
                        writer.write(Utils.bulkString(RdbUtils.RDBkeyValueHashMap.get(args[1])));
                        writer.flush();
                    }
                }

            } else {
                if (commandBytesLength == -1) {
                    writer.write(Config.NIL);
                    writer.flush();
                }
            }

        } else if (args[0].equalsIgnoreCase("config")) {

            if (args[1].equalsIgnoreCase("get")) {
                if (args[2].equalsIgnoreCase("dir")) {
                    if (commandBytesLength == -1) {
                        writer.write(Utils.encodeArray(new String[] { "dir", Config.dir }));
                        writer.flush();
                    }
                } else if (args[2].equalsIgnoreCase("dbfilename")) {
                    if (commandBytesLength == -1) {
                        writer.write(Utils.encodeArray(new String[] { "dbfilename", Config.dbfilename }));
                        writer.flush();
                    }
                } else {
                    if (commandBytesLength == -1) {
                        writer.write("-ERROR: Unknown configuration key arguments\r\n");
                        writer.flush();
                    }
                }

            } else {
                if (commandBytesLength == -1) {
                    writer.write("-ERROR: Unknown command or incorrect arguments\r\n");
                    writer.flush();
                }

            }
        } else if (args[0].equalsIgnoreCase("keys")) {

            if (Config.dbfilename.isEmpty() && Config.dir.isEmpty()) {
                if (commandBytesLength == -1) {
                    writer.write("-ERROR: RDB File not found\r\n");
                    writer.flush();
                }
            } else {

                if (args[1].equals("*")) {
                    System.out.println("keys: " + Arrays.toString(RdbUtils.getKeys()));
                    if (commandBytesLength == -1) {
                        writer.write(Utils.encodeArray(RdbUtils.getKeys()));
                        writer.flush();
                    }
                }
            }

        } else if (args[0].equalsIgnoreCase("info")) {
            if (Config.hostPort != -1 && !Config.hostName.isBlank()) {
                StringBuilder output = new StringBuilder();
                output.append("role:slave");
                output.append("\n");
                output.append("master_replid:").append("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
                output.append("\n");
                output.append("master_repl_offset:").append("0");

                if (commandBytesLength == -1) {
                    System.out.println("i am getting executed");
                    writer.write(Utils.bulkString(output.toString()));
                    writer.flush();
                }
            } else {
                if (commandBytesLength == -1) {
                    writer.write(Utils.bulkString("role:slave"));
                    writer.flush();
                }
            }

        } else if (args[0].equalsIgnoreCase("replconf")) {
            if (args[1].equalsIgnoreCase("listening-port")) {
                System.out.println("Repl listening port: " + Integer.parseInt(args[2]));
                writer.write("+OK" + Config.CRLF);
                writer.flush();
            } else if (args[1].equalsIgnoreCase("capa")) {
                System.out.println("capabilitles: " + args[2]);
                writer.write("+OK" + Config.CRLF);
                writer.flush();
            } else if (args[1].equalsIgnoreCase("getack")) {
                String command = Utils.RESP2format("REPLCONF ACK " + Config.bytesProcessedBySlave);
                writer.write(command);
                writer.flush();
            }
        } else {
            if (commandBytesLength == -1) {
                writer.write("-ERROR: Unknown command or incorrect arguments\r\n");
                writer.flush();
            }
            return;
        }
        if (commandBytesLength != -1) {
            Config.bytesProcessedBySlave += commandBytesLength;
        }
    }

    public static void handshake() throws Exception {

        try (Socket socket = new Socket(Config.hostName, Config.hostPort);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        ) {

            // stage 1 sending ping
            // writer.write(Utils.RESP2format("PING"));
            writer.write("*1\r\n$4\r\nPING\r\n");
            writer.flush();
            System.out.println(reader.readLine());

            // stage 2 sending replconf I
            writer.write(Utils.RESP2format("REPLCONF listening-port " + Config.port));
            writer.flush();
            System.out.println(reader.readLine());

            // stage 3 sending replconf II
            writer.write(Utils.RESP2format("REPLCONF capa psync2"));
            writer.flush();
            System.out.println(reader.readLine());

            // configuring replica to send ACK
            // writer.write(Utils.RESP2format("REPLCONF GETACK *"));
            // writer.flush();

            writer.write(Utils.RESP2format("PSYNC ? -1"));
            writer.flush();
            System.out.println("fullresync: " + reader.readLine());

            String str = reader.readLine();

            int rdbSize = 0;
            if (str.startsWith("$")) {
                rdbSize = Integer.parseInt(str.substring(1));
            }
            System.out.println("rdb file size: " + rdbSize);
            int i = 0;
            for (i = 0; i < rdbSize - 1; i++) {
                reader.read();
            }
            // while ((char) reader.read() != '*') {
            // reader.read();
            // System.out.println(i++);
            // }

            // String line = reader.readLine();
            // System.out.println("this is line" + line);

            // int commandArrayLength =
            // Integer.parseInt((reader.readLine()).substring(1).trim());
            // String[] commandArray = new String[commandArrayLength];
            // for (i = 0; i < commandArrayLength; i++) {
            // String lengthString = reader.readLine();
            // int commandLength = 0;
            // if (lengthString.startsWith("$")) {
            // commandLength = Integer.parseInt(lengthString.substring(1));
            // commandArray[i] = reader.readLine();
            // if (commandArray[i].length() != commandLength) {
            // throw new Exception("invalid RESP string, length dosne't match");
            // }
            // }
            // }
            // System.out.println("first command array : " + Arrays.toString(commandArray));
            // SlaveProfile.processCommand(writer, commandArray);

            Config.isHandshakeComplete = true;
            System.out.println("i got executed here 222");
            // System.out.println("received some command : " +
            // Config.bytesProcessedBySlave);
            Config.bytesProcessedBySlave = 0;
            while (true) {
                String reqLength;
                while ((reqLength = reader.readLine()) != null) {
                    System.out.println("reqLength: " + reqLength);
                    int commandLength = 0;
                    commandLength += reqLength.getBytes().length + 2;
                    // Parse the RESP array
                    if (reqLength.startsWith("*")) {
                        int numArgs = Integer.parseInt(reqLength.substring(1));
                        String[] args = new String[numArgs];
                        for (i = 0; i < numArgs; i++) {
                            String lineLength = reader.readLine();
                            commandLength += lineLength.getBytes().length + 2;
                            if (!lineLength.startsWith("$")) {
                                writer.write("-ERROR: Invalid RESP format\r\n");
                                writer.flush();
                                continue;
                            }
                            int length = Integer.parseInt(lineLength.substring(1));
                            args[i] = reader.readLine();
                            commandLength += length + 2;
                            if (args[i].length() != length) {
                                writer.write("-ERROR: Length mismatch\r\n");
                                writer.flush();
                                continue;
                            }
                        }
                        System.out.println("MASTER: " + Arrays.toString(args));
                        System.out.println("command length " + commandLength);
                        SlaveProfile.processCommand(writer, args, commandLength);
                    }
                }
            }

        } catch (IOException e) {
            System.out.println("Something went wrong while establishing handshake");
        }

    }

}