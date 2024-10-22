# BT
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ElectricityConsumption {

  // Mapper class để đọc và xử lý dữ liệu
  public static class YearConsumptionMapper
       extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text year = new Text();
    private DoubleWritable consumption = new DoubleWritable();

    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
      String[] lines = value.toString().split("\\r?\\n");
      for (int i = 0; i < lines.length; i += 13) {
        // Dòng đầu tiên là năm
        year.set(lines[i]);
        double totalConsumption = 0.0;

        // Tính tổng sản lượng điện tiêu thụ trong 12 tháng
        for (int j = 1; j <= 12; j++) {
          totalConsumption += Double.parseDouble(lines[i + j]);
        }

        // Tính trung bình
        double averageConsumption = totalConsumption / 12;
        consumption.set(averageConsumption);

        // Phát ra năm và giá trị trung bình
        context.write(year, consumption);
      }
    }
  }

  // Reducer class để tìm các năm có mức tiêu thụ trung bình lớn hơn 30
  public static class AvgConsumptionReducer
       extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<DoubleWritable> iterator = values.iterator();
      if (iterator.hasNext()) {
        double avgConsumption = iterator.next().get();

        // Chỉ ghi ra những năm có mức tiêu thụ trung bình lớn hơn 30
        if (avgConsumption > 30) {
          context.write(key, new DoubleWritable(avgConsumption));
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // Cấu hình job Hadoop
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "electricity consumption");

    job.setJarByClass(ElectricityConsumption.class);
    job.setMapperClass(YearConsumptionMapper.class);
    job.setReducerClass(AvgConsumptionReducer.class);

    // Đặt kiểu dữ liệu đầu ra cho key và value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    // Đặt đường dẫn file đầu vào và đầu ra
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Chạy job và chờ hoàn thành
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
