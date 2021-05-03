import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.beam.PublisherOptions;
import com.google.cloud.pubsublite.beam.PubsubLiteIO;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LitePublicTopicTemplate {

  public interface LitePublicTopicTemplateOptions extends PipelineOptions, StreamingOptions {
    @Description(
        "Your output Pub/Sub Lite topic, e.g. projects/123456789/locations/us-central1-b/topics/fish")
    @Required
    String getPubsubLiteTopic();

    void setPubsubLiteTopic(String value);
  }

  private static final Logger LOG = LoggerFactory.getLogger(LitePublicTopicTemplate.class);

  public static void main(String[] args) {

    LitePublicTopicTemplateOptions options =
        PipelineOptionsFactory.fromArgs(args).as(LitePublicTopicTemplateOptions.class);

    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    PublisherOptions publisherOptions =
        PublisherOptions.newBuilder()
            .setTopicPath(TopicPath.parse(options.getPubsubLiteTopic()))
            .build();

    pipeline
        .apply(
            "Read From Pub/Sub",
            PubsubIO.readStrings()
                .fromTopic("projects/pubsub-public-data/topics/taxirides-realtime"))
        .apply(
            "Transform message",
            MapElements.into(TypeDescriptor.of(PubSubMessage.class))
                .via(
                    (String m) -> {
                      Message message =
                          Message.builder().setData(ByteString.copyFromUtf8(m)).build();
                      LOG.info("Created: " + m);
                      return message.toProto();
                    }))
        .apply("Write to Pub/Sub Lite", PubsubLiteIO.write(publisherOptions));

    pipeline.run();
  }
}
