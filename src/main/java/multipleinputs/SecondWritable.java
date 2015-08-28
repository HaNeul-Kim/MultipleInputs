package multipleinputs;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Ha Neul, Kim
 */
public class SecondWritable implements Writable {

    private String value;
    private String additional;

    public SecondWritable() {
        this.value = "TEST";
        this.additional = "";
    }

    public SecondWritable(String val, String addi) {
        this.value = val;
        this.additional = addi;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (null == in) {
            throw new IllegalArgumentException("in cannot be null");
        }
        String value = in.readUTF();
        String addi = in.readUTF();
        this.value = value.trim();
        this.additional = addi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (null == out) {
            throw new IllegalArgumentException("out cannot be null");
        }
        out.writeUTF(this.value);
        out.writeUTF(this.additional);
    }

    @Override
    public String toString() {
        return value + " " + additional;
    }
}