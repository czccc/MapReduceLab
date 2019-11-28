import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TwoNum implements WritableComparable<TwoNum>
{
    private int a;
    private int b;
    public int getA() {return a;}
    public int getB() {return b;}

    public void set(int x, int y)
    {
        a = x;
        b = y;

    }
    @Override
    public void readFields(DataInput in) throws IOException
    {
        a = in.readInt();
        b = in.readInt();
    }
    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(a);
        out.writeInt(b);
    }
    @Override
    public int compareTo(TwoNum o)
    {
        if( a != o.a)
        {   return a < o.a? -1:1;}
        else if (b != o.b)
        {   return b > o.b? -1:1;}
        else
            return 0;
    }
}