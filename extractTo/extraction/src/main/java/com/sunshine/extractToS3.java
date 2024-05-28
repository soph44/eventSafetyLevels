package com.sunshine;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.io.File;
import java.io.IOException;
import java.lang.InterruptedException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
// import java.io.FileWriter;
// import org.json.simple.JSONObject;
// import com.google.gson.Gson;
// import com.google.gson.GsonBuilder;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;

public class extractToS3{
    final static String baseUrlCovid = "https://disease.sh/v3/covid-19/historical/usacounties/";

    public static AmazonS3 connectS3Client(){
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
        return s3;
    }

    public static void checkBucketExist(String bucketName, AmazonS3 s3){
        Boolean ifExists;
        Bucket b;

        ifExists = null;
        if (s3.doesBucketExistV2(bucketName)) {
            ifExists = true;
            System.out.println("Bucket Exists!");
           } else {
            try {
                b = s3.createBucket(bucketName);
                ifExists = false;
                System.out.println("Bucket Doesn't Exists! Initializing Bucket!");
            } catch (AmazonS3Exception e) {
                System.err.println(e.getErrorMessage());
            }
           }
    }

    public static void checkFileExists(String fileName){

    }

    public static void main(String[] args){
        // Create S3 client
        AmazonS3 s3 = connectS3Client();

        // Create S3 bucket if not existing
        String bucketName = "sunshine-covidapibucket-dev";
        checkBucketExist(bucketName, s3);
        
        // GET Covid data from API for past day
        String state = "california";    //args[0];
        String lastDays = "1";          //args[1];
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
            System.out.println("::fileKey : " + fileKey);
            try{
                System.out.format("::Uploading %s to S3 bucket %s...\n", fileKey, bucketName);
                s3.putObject(bucketName, fileKey, responseBody);
            } catch (AmazonServiceException e) {
                System.err.println(e.getErrorMessage());
                System.exit(1);
        }
        } catch (IOException e){
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e){
            e.printStackTrace();
            System.exit(1);
        }

    }
}
