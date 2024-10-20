import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import java.text.DecimalFormat;

public class WeekdayTransactionStats {

    public static class WeekdayMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) return;

            String date = fields[0];
            String[] amounts = fields[1].split(",");
            double purchaseAmt = Double.parseDouble(amounts[0]);
            double redeemAmt = Double.parseDouble(amounts[1]);

            // 计算星期几
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            try {
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(sdf.parse(date));
                String weekday = new SimpleDateFormat("EEEE").format(calendar.getTime());
                context.write(new Text(weekday), new Text(purchaseAmt + "," + redeemAmt));
            } catch (Exception e) {
                // 处理解析异常
            }
        }
    }

    public static class WeekdayReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Double[]> averages = new HashMap<>();
        private Map<String, Integer> counts = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalPurchase = 0;
            double totalRedeem = 0;
            int count = 0;

            for (Text value : values) {
                String[] amounts = value.toString().split(",");
                totalPurchase += Double.parseDouble(amounts[0]);
                totalRedeem += Double.parseDouble(amounts[1]);
                count++;
            }

            averages.put(key.toString(), new Double[]{totalPurchase, totalRedeem});
            counts.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String key : averages.keySet()) {
                Double[] totals = averages.get(key);
                int count = counts.get(key);
                double avgPurchase = totals[0] / count;
                double avgRedeem = totals[1] / count;

                context.write(new Text(key), new Text(avgPurchase + "," + avgRedeem));
            }
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) return;

            String weekday = fields[0];
            String[] amounts = fields[1].split(",");
            double purchaseAmt = Double.parseDouble(amounts[0]);

            context.write(new DoubleWritable(purchaseAmt), new Text(weekday + "\t" + amounts[1]));
        }
    }

    public static class DescendingCountComparator extends WritableComparator{
        protected DescendingCountComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable val1 = (IntWritable) w1;
            IntWritable val2 = (IntWritable) w2;
            return val2.compareTo(val1);
        }
    }


    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        private DecimalFormat decimalFormat = new DecimalFormat("#.##");
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                // value 的格式是 weekday TAB redeemAmt
                String[] parts = value.toString().split("\t");
                if (parts.length < 2) continue;

                String weekday = parts[0];
                String redeemAmt = parts[1];

                String formattedPurchase = decimalFormat.format(key.get());
                String formattedRedeem = decimalFormat.format(Double.parseDouble(redeemAmt));
                
                context.write(new Text(weekday), new Text(formattedPurchase + "," + formattedRedeem));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Weekday Transaction Stats");

        job1.setJarByClass(WeekdayTransactionStats.class);
        job1.setMapperClass(WeekdayMapper.class);
        job1.setReducerClass(WeekdayReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/tmp"));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sort By CashIn");

        job2.setJarByClass(WeekdayTransactionStats.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);

        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setSortComparatorClass(DescendingCountComparator.class);
        job2.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/result"));

    
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}