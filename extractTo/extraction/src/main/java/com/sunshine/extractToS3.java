package com.sunshine;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.InterruptedException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.Calendar;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;

/**
 * File: extractToS3.java
 * Author: SonnyP
 * Main method creates the AWS S3 client connection and checks that the buckets exists or creates them
 * Make calls to a COVID database API and a FLU database API.
 * Pushes today's (Covid) or this week's (Flu) data to AWS S3.
 * Covid API doc https://disease.sh/docs/#/COVID-19%3A%20JHUCSSE/get_v3_covid_19_historical_usacounties__state_
 * Flu API doc https://cmu-delphi.github.io/delphi-epidata/api/fluview.html

*/
public class extractToS3{
    final static String baseUrlCovid = "https://disease.sh/v3/covid-19/historical/usacounties/";
    final static String baseUrlFlu = "https://api.delphi.cmu.edu/epidata/fluview/";
    private static final Logger LOGGER = Logger.getLogger(extractToS3.class.getName());

    public static AmazonS3 connectS3Client(){
        // Creates connection and resource for AWS S3
        LOGGER.log(Level.INFO, "::connectS3 Client Start");
        try{
            final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
            LOGGER.log(Level.INFO, "::connectS3 Client End - AWS Connect S3 Successful");
            return s3;
        } catch (AmazonS3Exception e) {
            System.err.println(e.getErrorMessage());
            LOGGER.log(Level.INFO, "::connectS3 Client End - Error: " + e.getErrorMessage() + e.getErrorCode());
            e.printStackTrace();
            System.exit(1);
            throw e;
        }
        
    }

    public static void checkBucketExist(String bucketName, AmazonS3 s3){
        // Check S3 bucket exists and create if not
        
        LOGGER.log(Level.INFO, "::checkBucketExist Start: " + bucketName);
        Boolean ifExists;
        Bucket b;

        ifExists = null;
        if (s3.doesBucketExistV2(bucketName)) {
            ifExists = true;
            System.out.println("::" + bucketName + "Bucket Exists!");
            LOGGER.log(Level.INFO, "::checkBucketExist End: Bucket Exists");
           } else {
            try {
                b = s3.createBucket(bucketName);
                ifExists = false;
                System.out.println("::" + bucketName + "Bucket Doesn't Exists! Initializing Bucket!");
                LOGGER.log(Level.INFO, "::checkBucketExist End: Bucket Does Not Exist. Bucket Created.");
            } catch (AmazonS3Exception e) {
                System.err.println(e.getErrorMessage());
                LOGGER.log(Level.INFO, "::checkBucketList End - Error: " + e.getErrorMessage() + e.getErrorCode());
                e.printStackTrace();
                System.exit(1);
            }
           }
    }

    public static void loadCovidData(String state, AmazonS3 s3, String bucketName){
        // GET Covid data from API for past day
        // Data available to the county level
        LOGGER.log(Level.INFO, "::loadCovidData Start: State " + state + " bucketName " + bucketName);

        if (state.contains("_")) {
            state = state.replace("_", "%20");
        }
        String lastDays = "1";
        String urlCovid = baseUrlCovid + state + "?lastdays=" + lastDays;      

        // GET from Covid API
        try{
            URI uri = URI.create(urlCovid);
            HttpRequest request = HttpRequest.newBuilder()
				.uri(uri)
				.method("GET", HttpRequest.BodyPublishers.noBody())
				.build();

            HttpResponse<String> response = HttpClient.newHttpClient().send(request, BodyHandlers.ofString());
            String responseBody = response.body();
            // System.out.println(responseBody);

            // PUT Covid JSON into S3 Bucket; overwrites existing if re-ran
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String dateStamp = dateFormat.format(new Date());
            String fileKey = "covid/" + state + "/" + dateStamp + "_last" + lastDays;
            LOGGER.log(Level.INFO, "::loadCovidData: Uploading with Key: " + fileKey);
            try{
                s3.putObject(bucketName, fileKey, responseBody);
            } catch (AmazonServiceException e) {
                LOGGER.log(Level.SEVERE, "::loadCovidData End - Error: " + e.getErrorMessage() + e.getErrorCode());
                // System.err.println(e.getErrorMessage());
                e.printStackTrace();
                System.exit(1);
                throw e;
        }
        } catch (IOException e){
            LOGGER.log(Level.SEVERE, e.toString());
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e){
            LOGGER.log(Level.SEVERE, e.toString());
            e.printStackTrace();
            System.exit(1);
        }
        LOGGER.log(Level.INFO, "::loadCovidData End");

    }

    public static void loadFluData(String region, AmazonS3 s3, String bucketName, String apiKey){
        // GET Influenza data from API for past day
        // Data available to the regional level
        // Example: https://api.delphi.cmu.edu/epidata/fluview/?regions=nat&epiweeks=201501

        LOGGER.log(Level.INFO, "::loadFluData Start: Region " + region + " bucketName " + bucketName);

        String epiWeek = "00";
        region = "hhs" + region;
        Calendar instance=Calendar.getInstance(); 
        int year = instance.get(Calendar.YEAR);
        int week = instance.get(Calendar.WEEK_OF_YEAR) - 2;
        if (week < 10){
            epiWeek = String.valueOf(year) + "0" + String.valueOf(week);
        } else {
            epiWeek = String.valueOf(year) + String.valueOf(week);
        }

        String urlFlu = baseUrlFlu + "?regions=" + region + "&epiweeks=" + epiWeek + "&auth=" + apiKey;
        // System.out.println(urlFlu);      

        // GET from Flu API
        try{

            URI uri = URI.create(urlFlu);
            HttpRequest request = HttpRequest.newBuilder()
				.uri(uri)
                // .header("api_key", "094742658a69e")
				.method("GET", HttpRequest.BodyPublishers.noBody())
				.build();
            
            HttpResponse<String> response = HttpClient.newHttpClient().send(request, BodyHandlers.ofString());
            String responseBody = response.body();
            // System.out.println(responseBody);

            // PUT Flu JSON into S3 Bucket; overwrites existing if re-ran
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String dateStamp = dateFormat.format(new Date());
            String fileKey = "flu/" + region + "/" + dateStamp;
            LOGGER.log(Level.INFO, "::loadFluData: Uploading with Key: " + fileKey);
            // System.out.println("::fileKey : " + fileKey);
            try{
                // System.out.format("::Uploading %s to S3 bucket %s...\n", fileKey, bucketName);
                s3.putObject(bucketName, fileKey, responseBody);
            } catch (AmazonServiceException e) {
                LOGGER.log(Level.SEVERE, "::loadFludData End - Error: " + e.getErrorMessage() + e.getErrorCode());
                // System.err.println(e.getErrorMessage());
                System.exit(1);
             }
        } catch (IOException e){
            LOGGER.log(Level.SEVERE, "::loadFluData End in error - " + e.toString());
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e){
            LOGGER.log(Level.SEVERE, "::loadFluData End in error - " + e.toString());
            e.printStackTrace();
            System.exit(1);
        }
    
        LOGGER.log(Level.INFO, "::loadFluData END");

    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        // Set logger
        LOGGER.setLevel(Level.INFO);
        LOGGER.log(Level.INFO, "::Main Start");
        // Create S3 client
        AmazonS3 s3 = connectS3Client();

        // Create S3 bucket if not existing
        String covidBucketName = "sunshine-covidapibucket-dev";
        String fluBucketName = "sunshine-fluapibucket-dev";
        checkBucketExist(covidBucketName, s3);
        checkBucketExist(fluBucketName, s3);
        
        // Creates array list of states and the state region
        List<String[]> states = new ArrayList<>();
        try{
            BufferedReader br = new BufferedReader(new FileReader("refTables/states.csv"));
            String line = "";  
            String DELIMITER = ",";  
            while ((line = br.readLine()) != null) {
                String[] values = line.split(DELIMITER);
                states.add(values);
            }
            states.remove(0);
            br.close();
        } catch (IOException e){
            LOGGER.log(Level.SEVERE, "::Main End in error - " + e.toString());
            e.printStackTrace();
            System.exit(1);
        }
        
        // parses out api key from credentials file
        JSONParser jsonParser = new JSONParser();
        String fluApiKey = "";
        try {
            JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("keys/apikey.json"));
            fluApiKey = (String) jsonObject.get("delphiepidata");
            // System.out.println("::fluApiKey: " + fluApiKey);
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "::Main End in error - " + e.toString());
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "::Main End in error - " + e.toString());
            e.printStackTrace();
            System.exit(1);
        } catch (ParseException e) {
            LOGGER.log(Level.SEVERE, "::Main End in error - " + e.toString());
            e.printStackTrace();
            System.exit(1);
        }

        // loops through all states and makes API calls for COVID and FLU data
        // pushes json responses to AWS S3
        for (String[] state : states){
            // System.out.println("::State: " + state[0] + " ::Region: " + state[2]);
            loadCovidData(state[0], s3, covidBucketName);
            loadFluData(state[2], s3, fluBucketName, fluApiKey);
        }
        LOGGER.log(Level.INFO, "::Main End");
    }
}
