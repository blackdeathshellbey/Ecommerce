package emse.aws;

import java.util.*;


public class WorkerOrganizer {

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new WorkerProcessor(), 1, 60000);
    }


}
