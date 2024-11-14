package com.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParser;

public class DailyFundMapper extends Mapper<Object, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(DailyFundMapper.class);
    private Text Date = new Text();
    private Text Flow = new Text();
    private boolean firstLine = true;
    private CSVParser csvParser;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.isEmpty()) {
            return;
        }

        if (firstLine && line.contains("report_date")) {
            firstLine = false;
            return;
        }

        String[] columns = parseCsvLine(line);
        if (columns.length < 9) {
            return;
        }

        String reportedDate = columns[1].trim();
        String input = columns[4].trim();
        String output = columns[8].trim();
        if (input.isEmpty()) {
            input = "0";
        }
        if (output.isEmpty()) {
            output = "0";
        }
        
        Date.set(reportedDate);
        Flow.set(input + "," + output);
        context.write(Date, Flow);
    }

    private String[] parseCsvLine(String line) {
        // CSV parsing logic can be extracted here
        return line.split(",");
    }
}
