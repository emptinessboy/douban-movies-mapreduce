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
        String[] data = new String[9];

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
        data[0] = m.replaceAll("").trim();


//        //判断读取的行 InputSplit是否包含“[” 和 “]” 或读取的行为空
//        if (line.indexOf("[") == 0 || line.indexOf("]") == 0 || line.trim().isEmpty()) {
//            return;
//        }
//        //删除逗号
//        line = line.substring(0, line.length() - 1);
//        //删除逗号时可能会删掉每行最后的"}"就是判断数据是否符合json格式这里我们要加一层判断否则解析json数据会出错
//        if (!line.endsWith("}")) {
//            line = line.concat("}");
//        }

        //fix auto type
        //line.replaceAll("@","");
//        System.out.println(line);

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
        data[1] = movie;
        //director
        JSONArray directors = jo.getJSONArray("director");
        for (int i = 0; i < directors.size(); i++) {
            if (i != 0) {
                data[2] += ",";
            }
            data[2] += directors.getJSONObject(i).getString("name");
        }

        //director
        JSONArray actors = jo.getJSONArray("actor");
        for (int i = 0; i < actors.size(); i++) {
            if (i != 0) {
                data[2] += ",";
            }
            data[2] += actors.getJSONObject(i).getString("name");
        }
//        data[0] = jo.getJSONObject("director").getString("name");
//        data[1] = name.trim();
//        data[2] = jsonObject.getString("actors");
//        data[3] = jsonObject.getString("time");
//        data[4] = jsonObject.getString("score");
        //循环判空
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
