package cs585.creditcard;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents a 5-dimensional point for credit card customer data:
 * (avg_credit_limit, total_credit_cards, total_visits_bank, total_visits_online, total_calls_made)
 */
public class Point5D implements Writable {

    public double[] dims = new double[5];

    public Point5D() {}

    public Point5D(double d0, double d1, double d2, double d3, double d4) {
        dims[0] = d0; dims[1] = d1; dims[2] = d2; dims[3] = d3; dims[4] = d4;
    }

    public static Point5D fromString(String line) {
        String[] parts = line.trim().split(",");
        return new Point5D(
            Double.parseDouble(parts[0]),
            Double.parseDouble(parts[1]),
            Double.parseDouble(parts[2]),
            Double.parseDouble(parts[3]),
            Double.parseDouble(parts[4])
        );
    }

    public double distanceTo(Point5D other) {
        double sum = 0;
        for (int i = 0; i < 5; i++) {
            double diff = this.dims[i] - other.dims[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public void add(Point5D other) {
        for (int i = 0; i < 5; i++) this.dims[i] += other.dims[i];
    }

    public void divide(long count) {
        for (int i = 0; i < 5; i++) this.dims[i] /= count;
    }

    public int nearestCenter(Point5D[] centers) {
        int nearest = 0;
        double minDist = Double.MAX_VALUE;
        for (int i = 0; i < centers.length; i++) {
            double dist = this.distanceTo(centers[i]);
            if (dist < minDist) { minDist = dist; nearest = i; }
        }
        return nearest;
    }

    public boolean hasConverged(Point5D other, double threshold) {
        return distanceTo(other) <= threshold;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (double d : dims) out.writeDouble(d);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < 5; i++) dims[i] = in.readDouble();
    }

    @Override
    public String toString() {
        return String.format("%.4f,%.4f,%.4f,%.4f,%.4f",
            dims[0], dims[1], dims[2], dims[3], dims[4]);
    }
}
