package vf.dfpl;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.values.PCollection;

public class bigqupd {
    public interface LoadCSVData extends PipelineOptions
    {
        @Description("Path of the file to read from")
        @Default.String("gs://sjdata/Cust.csv")
        String getInputFile();
        void setInputFile(String value);

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Required
        String getOutput();
        void setOutput(String value);
    }

    //Run Pipeline For Data Load
    static void runPipeLine(LoadCSVData options)
    {
        Pipeline p = Pipeline.create(options);

        //Read Data from input File
        PCollection<String> pc = p.apply("Read Lines", TextIO.read().from(options.getInputFile()));
        pc.apply("WriteLines", TextIO.write().to(options.getOutput()));
        p.run().waitUntilFinish();
    }

    public static void main(String[] args)
    {
        LoadCSVData options = PipelineOptionsFactory.fromArgs(args).withValidation().as(LoadCSVData.class);
        runPipeLine(options);
    }
}