package cs455.hadoop.customresearch;

public class FlightCounts {
    private int totalFlights = 0;
    private int totalDelay = 0;

    public FlightCounts() {
    }

    public FlightCounts(int totalFlights, int totalDelay) {
        this.totalFlights = totalFlights;
        this.totalDelay = totalDelay;
    }

    public int getTotalFlights() {
        return totalFlights;
    }

    public int getTotalDelay() {
        return totalDelay;
    }

    public void incrementValues(int totalDelay, int totalFlightCounts) {
        this.totalFlights += totalFlightCounts;
        this.totalDelay += totalDelay;
    }

    public int getAverage() {
        return (int) (((double)totalDelay / totalFlights) *100);
    }
}
