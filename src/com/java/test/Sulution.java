package com.java.test;

import java.nio.file.Paths;

public class Sulution {
    public static void main(String[] args) {
        LogParser logParser = new LogParser(Paths.get("/Users/evgeniygulak/IdeaProjects/JavaRushTasks/4.JavaCollections/src/com/javarush/task/task39/task3913/logs/"));
        System.out.println(logParser.execute("get date for user = \"Vasya Pupkin\" and date between \"19.03.2016 00:00:00\" and \"null\""));
    }
}