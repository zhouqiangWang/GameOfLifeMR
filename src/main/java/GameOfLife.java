import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.io.FileUtils;


import java.io.File;
import java.io.IOException;
import java.util.Arrays;


public class GameOfLife {
    public static int[] gameOfLife1Row(int[][] rows, int rowNumber) {
//        System.err.println("-----------------------" + rowNumber);
//        System.err.println(Arrays.toString(rows[0]));
//        System.err.println(Arrays.toString(rows[1]));
//        System.err.println(Arrays.toString(rows[2]));
//        System.err.println("-----------------------");
        int[] next = new int[InputGenerator.COLUMN];
        int[][] directions = new int[][] {
                {1, 0}, {1, 1}, {0, 1}, {-1, 1}, {-1, 0}, {-1, -1}, {0, -1}, {1, -1}
        };
        int target = 1;
        if (rowNumber == 1) {
            target = 0;
        } else if (rowNumber == InputGenerator.ROW) {
            target = 2;
        }
        int col = rows.length;
        for(int i = 0; i < InputGenerator.COLUMN; i++) {
            int count1 = 0;
            for (int[] dir : directions) {
                if(target + dir[0] >=0 && target + dir[0] < col && i + dir[1] >= 0 && i + dir[1] < InputGenerator.COLUMN
                    && rows[target + dir[0]][i+dir[1]] == 1) {
                    count1 ++;
                }
            }

            if (count1 == 3) {
                next[i] = 1;
            } else if (count1 == 2) {
                next[i] = rows[target][i];
            }
        }
        return next;
    }
    public static class GameMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private int n = 0;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("key = " + key.toString());
            System.err.println("value = " + value.toString());

            key.set(n++);
            context.write(key, value);
        }
    }

    public static class NextLifeReducer extends Reducer<LongWritable, Text, Text, Text> {
        private final Text empty = new Text();
        int[][] board = new int[3][InputGenerator.COLUMN];

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {

            int row = (int)key.get();
            System.out.println("row = " + row);

            for (Text val : values) {
                String[] line = val.toString().split(" ");
                int col = 0;
                if (row <= 2) {
                    for (String number : line) {
                        board[row][col++] = Integer.parseInt(number);
                    }
                    System.out.println(Arrays.toString(board[row]));
                } else {
                    board[0] = board[1];
                    board[1] = board[2];
                    for (String number : line) {
                        board[2][col++] = Integer.parseInt(number);
                    }
                    System.out.println(Arrays.toString(board[2]));
                }
            }

            if (row == 0) {
                return;
            }
            int[] nextLine = gameOfLife1Row(board, row);
            System.err.println(Arrays.toString(nextLine));

            Text newKey = new Text();
            newKey.set(Arrays.toString(nextLine));

            context.write(newKey, empty);

            if (row == InputGenerator.ROW - 1) {
                nextLine = gameOfLife1Row(board, row + 1);
                System.err.println(Arrays.toString(nextLine));
                newKey.set(Arrays.toString(nextLine));

                context.write(newKey, empty);
            }
        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: GameOfLife <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Game of life");
        job.setJarByClass(GameOfLife.class);
        job.setMapperClass(GameMapper.class);
//        job.setCombinerClass(NextLifeReducer.class);
        job.setReducerClass(NextLifeReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        String outDir = otherArgs[otherArgs.length - 1];
        File out = new File(outDir);
        if (out.exists()) {
            FileUtils.deleteDirectory(out);
        }

        FileOutputFormat.setOutputPath(job,
                new Path(outDir));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
