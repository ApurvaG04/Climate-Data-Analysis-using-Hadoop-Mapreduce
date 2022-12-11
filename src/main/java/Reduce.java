import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.DecimalFormat;
import java.util.Iterator;

import java.io.IOException;
import java.util.StringTokenizer;

import static org.apache.hadoop.util.functional.RemoteIterators.foreach;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context
    )throws IOException, InterruptedException {
        DecimalFormat df = new DecimalFormat("#.00");
        float temp_sum = 0;
        float dewpt_sum = 0;
        float pressure_sum = 0;
        float humidity_sum = 0;
        float windSpd_sum = 0;
        int count = 0;
        float min_temp = Integer.MAX_VALUE;
        float max_temp = Integer.MIN_VALUE;
        float min_dewpt = Integer.MAX_VALUE;
        float max_dewpt = Integer.MIN_VALUE;
        float min_pressure = Integer.MAX_VALUE;
        float max_pressure = Integer.MIN_VALUE;
        float min_humidity = Integer.MAX_VALUE;
        float max_humidity = Integer.MIN_VALUE;
        float min_windspd = Integer.MAX_VALUE;
        float max_windspd = Integer.MIN_VALUE;

        for(Text val: values){
            String[] line_tokens = val.toString().split(" ");
            float temp = Float.parseFloat(line_tokens[0]);
            float dewpt = Float.parseFloat(line_tokens[1]);
            float pressure = Float.parseFloat(line_tokens[2]);
            float humidity = Float.parseFloat(line_tokens[3]);
            float windspd = Float.parseFloat(line_tokens[4]);
            //sum of attributes for each month or for each unique key
            temp_sum += temp;
            dewpt_sum += dewpt;
            pressure_sum += pressure;
            humidity_sum += humidity;
            windSpd_sum += windspd;
            count+=1;
            //min and maximum values for each month of each year for each location.
            if(temp<min_temp)
                min_temp=Float.valueOf(df.format(temp));
            if(temp>max_temp)
                max_temp= Float.valueOf(df.format(temp));

            if(dewpt<min_dewpt)
                min_dewpt=Float.valueOf(df.format(dewpt));
            if(dewpt>max_dewpt)
                max_dewpt=Float.valueOf(df.format(dewpt));

            if(pressure<min_pressure)
                min_pressure=Float.valueOf(df.format(pressure));
            if(pressure>max_pressure)
                max_pressure=Float.valueOf(df.format(pressure));

            if(humidity<min_humidity)
                min_humidity=Float.valueOf(df.format(humidity));
            if(humidity>max_humidity)
                max_humidity=Float.valueOf(df.format(humidity));

            if(windspd<min_windspd)
                min_windspd=Float.valueOf(df.format(windspd));
            if(windspd>max_windspd)
                max_windspd=Float.valueOf(df.format(windspd));
        }
        //averages of each attribute for each month of each year for each location.
        float avg_temp = Float.valueOf(df.format(temp_sum / count));
        float avg_dewpt = Float.valueOf(df.format(dewpt_sum / count));
        float avg_pressure = Float.valueOf(df.format(pressure_sum / count));
        float avg_humidity = Float.valueOf(df.format(humidity_sum / count));
        float avg_windspd = Float.valueOf(df.format(windSpd_sum / count));

        //reducer output generation.
        String output = "temp: ".concat(String.valueOf(min_temp)).concat(" ").concat(String.valueOf(avg_temp)).concat(" ").
                concat(String.valueOf(max_temp)).concat("   dewpt: ").concat(String.valueOf(min_dewpt)).concat(" ").
                        concat(String.valueOf(avg_dewpt)).concat(" ").concat(String.valueOf(max_dewpt)).concat("    pressure: ").
                concat(String.valueOf(min_pressure)).concat(" ").concat(String.valueOf(avg_pressure)).concat(" ").
                        concat(String.valueOf(max_pressure)).concat("   humidity: ").concat(String.valueOf(min_humidity)).concat(" ").
                concat(String.valueOf(avg_humidity)).concat(" ").concat(String.valueOf(max_humidity)).concat("  windspeed: ").
                concat(String.valueOf(min_windspd)).concat(" ").concat(String.valueOf(avg_windspd)).concat(" ").concat(String.valueOf(max_windspd));
        Text value_output = new Text(output);
        System.out.println(key + " " + value_output);
        context.write(key, value_output);


    }
}
