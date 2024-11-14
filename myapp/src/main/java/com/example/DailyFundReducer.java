package com.example;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DailyFundReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(DailyFundReducer.class);

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double allIn = 0.0;
        double allOut = 0.0;

        for (Text val : values) {
            String[] amounts = val.toString().split(",");
            if (amounts.length < 2) {
                continue;
            }
            try {
                double in = Double.parseDouble(amounts[0]);
                double out = Double.parseDouble(amounts[1]);
                allIn += in;
                allOut += out;
            } catch (NumberFormatException e) {
                logger.error("NumberFormatException: " + e.getMessage(), e);
                continue;
            }
        }

        result.set(allIn + "," + allOut);
        context.write(key, result);
    }
}
