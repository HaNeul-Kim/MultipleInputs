import org.apache.hadoop.util.ProgramDriver;

/**
 * @author Ha Neul, Kim
 */
public class MapReduceDriver {

    public static void main(String argv[]) {
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("multipleInputs", MultipleInputsDriver.class, "사용자가 정의한 수식을 이용하여 컬럼별 계산을 할 수 있는 MapReduce ETL");
            programDriver.driver(argv);
            System.exit(Constants.JOB_SUCCESS);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(Constants.JOB_FAIL);
        }
    }
}

