package shyamal.artifacia.christmas;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import clarifai2.api.ClarifaiBuilder;
import clarifai2.api.ClarifaiClient;
import clarifai2.dto.input.ClarifaiInput;
import clarifai2.dto.input.image.ClarifaiImage;
import clarifai2.dto.model.output.ClarifaiOutput;
import clarifai2.dto.prediction.Concept;

public class App 
{
	public static void main( String[] args )
	{
		System.out.println("startup...");
		TransportClient searchClient = null;
		ClarifaiClient ccClient = null;
		//read urls from data.txt 
		File urls = new File("data.txt");

		// on startup, default cluster name is elastic-search 
		try {
			//have used my API key for test purposes. Kindly use yours. 
			ccClient = new ClarifaiBuilder("VJXh4tl0b-MgNe9sHdc3qExN7dJW7OyQpKz9-6Sv", 
					"vV42tVrurO1DYEwv7daLTsMY22GG7P-8qSfkHx3W").buildSync();
			searchClient = new PreBuiltTransportClient(Settings.EMPTY)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			System.out.println(searchClient.toString());
		} catch (Exception e1) {
			e1.printStackTrace();
		}


		Scanner scan = null;
		try {
			scan = new Scanner(urls);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}

		while(scan.hasNextLine()){
			List<String> tags = new ArrayList<String>();
			String url = scan.nextLine();

			//make API call to clarifai for prediction results with a christmas concept
			List<ClarifaiOutput<Concept>> predictionResults =
					ccClient.getDefaultModels().generalModel() 
					// You can also do client.getModelByID("id") to get custom models
					.predict()
					.withInputs(
							ClarifaiInput.forImage(ClarifaiImage.of(url)).
							withConcepts(Concept.forID("Christmas")
									))
					.executeSync()
					.get();

			//parse the output in order to get tags 
			ClarifaiOutput<Concept> out = predictionResults.get(0);
			int numTags = out.data().size();
			for(int i = 0; i < numTags; i++) {
				tags.add(out.data().get(i).name());
			}
			System.out.println(tags.toString());

			//create json document for url:tags with maps
			Map <String, List<String>> json = new HashMap<String, List<String>>();
			json.put(url, tags);

			//Index the image document with index :christmas and type: images
			IndexResponse response = searchClient.prepareIndex("christmas", "images"
					).setSource(json).get();
			System.out.println(response.status().toString());

		}
	}
}
