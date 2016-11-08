package fr.eurecom.dsg.mapreduce.Stripes;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntMapWritable implements Writable {

    // TODO: add an internal field that is the real associative array
    HashMap<Text, Long> assAry = new HashMap<>();


    @Override
    public void readFields(DataInput in) throws IOException {

        // TODO: implement deserialization

        assAry.clear();

        LongWritable lw = new LongWritable();
        lw.readFields(in);

        for(int i = 0; i < lw.get(); i++) {
            Text text = new Text();
            text.readFields(in);
            lw.readFields(in);
            this.assAry.put(text, lw.get());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO: implement serialization
        LongWritable lw = new LongWritable();
        lw.set(this.assAry.size());
        lw.write(out);

        for (Text word : this.assAry.keySet()) {
            word.write(out);
            lw.set(this.assAry.get(word));
            lw.write(out);
        }
    }
}
