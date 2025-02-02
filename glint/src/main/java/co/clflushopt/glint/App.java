package co.clflushopt.glint;

import java.io.FileNotFoundException;

import co.clflushopt.glint.examples.NYCYellowTrips;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Welcome to the Glint query compiler");
        try {
            NYCYellowTrips.runParquetExample();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}