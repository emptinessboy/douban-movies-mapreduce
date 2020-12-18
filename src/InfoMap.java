import com.alibaba.fastjson.*;
import com.alibaba.fastjson.parser.ParserConfig;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class InfoMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static Text text = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // out put data
        String[] data = new String[7];

        //获取文件名字
        InputSplit inputSplit = (InputSplit) context.getInputSplit();
        String filename = ((FileSplit) inputSplit).getPath().getName();
        System.out.println("当前读取的文件名为：" + filename);

        // zheng ze biao da shi pi pei
        String pattern = "[^0-9]";
        // 创建 Pattern 对象
        Pattern r = Pattern.compile(pattern);
        // 现在创建 matcher 对象
        Matcher m = r.matcher(filename);
        // @排名
        data[0] = m.replaceAll("").trim();

        // read file
        String line = value.toString();

        ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
        //解析json数据
        JSONObject jo = JSON.parseObject(line);

        String movie = jo.getString("name");

        //判断movie字段是否为空
        if (movie == null || movie.trim().isEmpty()) {
            return;
        }
        // @电影名 movie
        data[1] = movie;

        // @导演 director
        JSONArray directors = jo.getJSONArray("director");
        for (int i = 0; i < directors.size(); i++) {
            if (i != 0) {
                data[2] += ",";
            }
            data[2] += directors.getJSONObject(i).getString("name");
        }

        // @主演 actors
        JSONArray actors = jo.getJSONArray("actor");
        for (int i = 0; i < actors.size(); i++) {
            if (i != 0) {
                data[2] += ",";
            }
            data[2] += actors.getJSONObject(i).getString("name");
        }

        // @上映日期 datePublished
        data[3] = jo.getString("datePublished");

        // @制片国家/地区 country
        data[4] = jo.getString("country");

        // @类型 mvtype
        data[5] = jo.getString("type");

        // @评分 rate
        data[6] = jo.getString("ratingValue");

        // @评分数量 ratingCount
        data[7] = jo.getString("ratingCount");

//        //循环判空
//        for (String i : data) {
//            if (i == null || i.equals("")) {
//                return;
//            }
//        }

        //分隔数据
        String end = "";
        for (String item : data) {
            end = end + item + "|";
        }
        end = end.substring(0, end.length() - 1);
        //将数据转为text类型并作为key输出
        text.set(end);
        context.write(text, NullWritable.get());
    }
}
//reduce不需要做任何处理直接输出即可
