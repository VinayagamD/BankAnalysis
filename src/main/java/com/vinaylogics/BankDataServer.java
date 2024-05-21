package com.vinaylogics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class BankDataServer {

    public static void main(String[] args) {
        try(ServerSocket listener = new ServerSocket(9090);
            Socket socket = listener.accept();
            BufferedReader br = new BufferedReader(new FileReader("/home/vinaylogics/bank_data.txt"));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)){
            System.out.printf("Got New Connection from "+ socket);
            String line;
            while ((line = br.readLine()) != null) {
                out.println(line);
                Thread.sleep(500);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
