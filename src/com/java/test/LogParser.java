package com.java.test;

import com.java.test.query.QLQuery;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LogParser implements QLQuery {
    private Path logDir;
    private List<LogEntity> logEntities = new ArrayList<>();
    private DateFormat simpleDateFormat = new SimpleDateFormat("d.M.yyyy H:m:s");

    public LogParser(Path logDir){
        this.logDir = logDir;
        scanLogFile();
    }

    private void scanLogFile(){
        try {
            Files.walkFileTree(logDir, new MyFileVisitor());
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public Set<Object> execute(String query) {
        Set<Object> result = new HashSet<>();
        String field1;
        String field2 = null;
        String value = null;
        Date after = null;
        Date before = null;

        Pattern pattern = Pattern.compile("get (ip|user|date|event|status)"
                + "( for (ip|user|date|event|status) = \"(.*?)\")?"
                + "( and date between \"(.*?)\" and \"(.*?)\")?");
        Matcher matcher = pattern.matcher(query);
        matcher.find();
        field1 = matcher.group(1);
        if(matcher.group(2) != null) {
            field2 = matcher.group(3);
            value = matcher.group(4);
            if(matcher.group(5) != null){
                try {
                    after = simpleDateFormat.parse(matcher.group(6));
                    before = simpleDateFormat.parse(matcher.group(7));
                } catch (ParseException e) {
                }
            }
        }

        if(field2 != null && value != null){
            String finalField = field2;
            String finalValue = value;
            Date finalAfter = after;
            Date finalBefore = before;
            if(!finalField.equals("date")) {
                result = logEntities.stream()
                        .filter(each -> dateBetweenDates(each.getDate(), finalAfter, finalBefore))
                        .filter(each -> (finalValue.equals(getCurrentValue(each, finalField).toString())))
                        .map(x -> getCurrentValue(x, field1))
                        .collect(Collectors.toSet());
            } else {
                result = logEntities.stream()
                        .filter(each -> dateBetweenDates(each.getDate(), finalAfter, finalBefore))
                        .filter(each -> {
                            try {
                                return each.getDate().getTime() == simpleDateFormat.parse(finalValue).getTime();
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            return false;
                        })
                        .map(x -> getCurrentValue(x, field1))
                        .collect(Collectors.toSet());
            }
        } else {
            switch (field1) {
                case "ip": {
                    return logEntities.stream()
                            .map(LogEntity::getIp)
                            .collect(Collectors.toSet());
                }
                case "user": {
                    return logEntities.stream()
                            .map(LogEntity::getUser)
                            .collect(Collectors.toSet());
                }
                case "date": {
                    return logEntities.stream()
                            .map(LogEntity::getDate)
                            .collect(Collectors.toSet());
                }
                case "event": {
                    return logEntities.stream()
                            .map(LogEntity::getEvent)
                            .collect(Collectors.toSet());
                }
                case "status": {
                    return logEntities.stream()
                            .map(LogEntity::getStatus)
                            .collect(Collectors.toSet());
                }
            }
        }
        return result;
    }
    private boolean dateBetweenDates(Date current, Date after, Date before) {
        if (after == null) {
            after = new Date(0);
        }
        if (before == null) {
            before = new Date(Long.MAX_VALUE);
        }
        return current.after(after) && current.before(before);
    }

    private Object getCurrentValue(LogEntity logEntity, String field) {
        Object value = null;
        switch (field) {
            case "ip": {
                value = logEntity.getIp();
                break;
            }
            case "user": {
                value = logEntity.getUser();
                break;
            }
            case "date": {
                value = logEntity.getDate();
                break;
            }
            case "event": {
                value = logEntity.getEvent();
                break;
            }
            case "status": {
                value = logEntity.getStatus();
                break;
            }
        }
        return value;
    }

    public class MyFileVisitor extends SimpleFileVisitor<Path>{
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if(file.toString().endsWith(".log"))
                readFile(file);
            return FileVisitResult.CONTINUE;
        }
    }

    public void readFile(Path path){
        String ip;
        String user;
        Date date;
        Event event;
        int eventAdditionalParameter = -1;
        Status status;

        try(BufferedReader reader = new BufferedReader(new FileReader(path.toFile()))){
            String line = null;
            while((line = reader.readLine()) != null){
                String[] params = line.split("\t");

                ip = params[0];
                user = params[1];
                date = readDate(params[2]);
                event = readEvent(params[3]);
                if(event.equals(Event.SOLVE_TASK) || event.equals(Event.DONE_TASK)){
                    eventAdditionalParameter = readAdditionalParameter(params[3]);
                }
                status = readStatus(params[4]);
                LogEntity logEntity = new LogEntity(ip,user,date, event,eventAdditionalParameter,status);
                logEntities.add(logEntity);
            }
        } catch (IOException | ParseException e){
            e.printStackTrace();
        }
    }

    private Date readDate(String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        return format.parse(date);
    }

    private Event readEvent(String stringEvent){
        Event event = null;
        if(stringEvent.contains("SOLVE_TASK")){
            event = Event.SOLVE_TASK;
        } else if(stringEvent.contains("DONE_TASK")){
            event = Event.DONE_TASK;
        } else {
            switch (stringEvent){
                case "LOGIN": {
                    event = Event.LOGIN;
                    break;}
                case "DOWNLOAD_PLUGIN": {
                    event = Event.DOWNLOAD_PLUGIN;
                    break;
                }
                case "WRITE_MESSAGE": {
                    event = Event.WRITE_MESSAGE;
                    break;
                }
            }
        }
        return event;
    }

    private int readAdditionalParameter(String stringEvent){
        String[] param = stringEvent.split(" ");
        return Integer.parseInt(param[1]);
    }

    private Status readStatus(String strStatus){
        Status status = null;
        if(strStatus.contains("ERROR")){
            status = Status.ERROR;
        } else if(strStatus.contains("FAILED")){
            status = Status.FAILED;
        } else if(strStatus.contains("OK")){
            status = Status.OK;
        }
        return status;
    }

    private class LogEntity{
        private String ip;
        private String user;
        private Date date;
        private Event event;
        private int eventAdditionalParameter;
        private Status status;

        public LogEntity(String ip, String user, Date date, Event event, int eventAdditionalParameter, Status status) {
            this.ip = ip;
            this.user = user;
            this.date = date;
            this.event = event;
            this.eventAdditionalParameter = eventAdditionalParameter;
            this.status = status;
        }

        @Override
        public String toString() {
            return "LogEntity{" +
                    "id='" + ip + '\'' +
                    ", user='" + user + '\'' +
                    ", date=" + date +
                    ", event=" + event +
                    ", eventAdditionalParameter=" + eventAdditionalParameter +
                    ", status=" + status +
                    '}';
        }

        public String getIp() {
            return ip;
        }
        public String getUser() {
            return user;
        }
        public Date getDate() {
            return date;
        }
        public Event getEvent() {
            return event;
        }
        public int getEventAdditionalParameter() {
            return eventAdditionalParameter;
        }
        public Status getStatus() {
            return status;
        }
    }
}