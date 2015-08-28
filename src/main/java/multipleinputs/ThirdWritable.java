package multipleinputs;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Ha Neul, Kim
 */
public class ThirdWritable implements Writable {

    private String value;

    public ThirdWritable() {
        this.value = "TEST";
    }

    public ThirdWritable(String val) {
        this.value = val;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (null == in) {
            throw new IllegalArgumentException("in cannot be null");
        }
        String value = in.readUTF();
        this.value = value.trim();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (null == out) {
            throw new IllegalArgumentException("out cannot be null");
        }
        out.writeUTF(this.value);
    }

    @Override
    public String toString() {
        return value;
    }
}