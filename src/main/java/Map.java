import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        int counter = 0;
        String key_str = null;
        String value_str = null;
        while(itr.hasMoreTokens() && counter<15){
            //split input file into tokens.
            String str = itr.nextToken();
            switch (counter) {
                case 0: //location
                    key_str = str;
                    break;
                case 3: //year
                    key_str = key_str.concat("_").concat(str);
                    break;
                case 4: //month
                    key_str = key_str.concat("_").concat(str);
                    break;
                case 8: //temperature
                    value_str = str.concat(" ");
                    break;
                case 9: //dew_point
                    value_str = value_str.concat(str).concat(" ");
                    break;
                case 10: //pressure
                    value_str = value_str.concat(str).concat(" ");
                    break;
                case 11: //humidity
                    value_str = value_str.concat(str).concat(" ");
                    break;
                case 12: //wind_speed
                    value_str = value_str.concat(str);

                    //output of mapper
                    Text keys = new Text(key_str);
                    Text values = new Text(value_str);
                    context.write(new Text(key_str), new Text(value_str));
                    break;
                default:
                    break;
            }
            counter++;
        }


    }

}
