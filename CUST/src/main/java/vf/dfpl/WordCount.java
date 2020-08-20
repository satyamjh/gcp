package vf.dfpl;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import java.util.Arrays;
import java.util.List;

public class WordCount
{
    public static void main(String... args)
    {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        //Read File
        PCollection<String> lines = pipeline.apply("Read from file", TextIO.read().from("sampletext.txt"));
        //Split Line
        PCollection<List<String>> WPerLine = lines.apply(MapElements.via(new SimpleFunction<String, List<String>>() {
            @Override
            public List<String> apply(String input)
            { return Arrays.asList(input.split(" "));}
        }));
        PCollection<String> words =WPerLine.apply(Flatten.iterables());

        //Count Words occurance
        PCollection<KV<String, Long>> wordcount = words.apply(Count.perElement());
        wordcount
                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String >() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return String.format("%s => %s", input.getKey(), input.getValue());
                    }
                }))
                .apply(TextIO.write().to("wordCount_Output"));

        pipeline.run().waitUntilFinish();
    }
}