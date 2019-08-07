import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class InputGenerator {
    public static final int COLUMN = 100;
    public static final int ROW = 10;
    public static void main(String[] args) throws IOException {
        FileWriter fw = new FileWriter("board.txt");
        FileWriter fwArray = new FileWriter("board_array.txt");
        Random random = new Random();

        fwArray.write("[");
        int[] arr = new int[COLUMN];
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < ROW; i++) {
            for (int j = 0; j < COLUMN; j++) {
                int rand = random.nextInt(2);
                arr[j] = rand;
                builder.append(rand);
                builder.append(' ');
            }
            builder.append('\n');
            fw.write(builder.toString());
            builder = new StringBuilder();

            fwArray.write(Arrays.toString(arr));
            if (i < ROW - 1) {
                fwArray.write(",");
            } else {
//                fwArray.write("\n");
            }
        }

        fwArray.write("]");

        fw.close();
        fwArray.close();
    }
}
