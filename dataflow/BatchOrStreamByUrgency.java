import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;


public class BatchOrStreamByUrgency {

  public static void main(String[] args) {

       PipelineOptions options = PipelineOptionsFactory.create();
       Pipeline p = Pipeline.create(options);

        // https://cloud.google.com/blog/products/data-analytics/how-to-efficiently-process-both-real-time-and-aggregate-data-with-dataflow

       // Separation Tags
		final TupleTag<TableRow> LowUrgency= new TupleTag<TableRow>(){};
		final TupleTag<TableRow> HighUrgency = new TupleTag<TableRow>(){};
		
		PCollectionTuple results = p.apply("ReadFromPubSub", PubsubIO.readStrings()
				.fromTopic(options.getTopic()))
				.apply("ConvertToTableRow", ParDo.of(new DoFn<String, TableRow>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						JSONObject json = new JSONObject(c.element());
						
						Integer urgency = json.getInt("Urgency");

						TableRow row = new TableRow();				

						row.set("Urgency", urgency);
						row.set("Timestamp", Instant.now().toString());
						
						// Sending row to each Tag using urgency as factor
						if (urgency >= 4) {
							c.output(row);
						}
						else {
							c.output(LowUrgency, row);
						}
					}}).withOutputTags(HighUrgency, TupleTagList.of(LowUrgency)));


        // Inserting via Load Jobs
        results.get(LowUrgency)
        .apply("WriteInBigQueryLoad", BigQueryIO.writeTableRows().to(tableLowUrgency)
            .withSchema(schema)
            .withMethod(Method.FILE_LOADS)
            .withTriggeringFrequency(Duration.standardMinutes(2))
            .withNumFileShards(1)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        // Inserting via Streaming
        results.get(HighUrgency)
            .apply("WriteInBigQueryStreaming", BigQueryIO.writeTableRows().to(tableHighUrgency)
            .withSchema(schema)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        // https://storage.googleapis.com/gweb-cloudblog-publish/images/pubsub.max-600x600.png

        // Alternate
        // Sending a percentage of the data for real-time analysis
        double rand = Math.random();
        if (rand<0.15){
            c.output(Streaming, row);
        }
        else{
		    c.output(row);
	    }
		}}).withOutputTags(Load, TupleTagList.of(Streaming)));