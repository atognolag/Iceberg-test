import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {

  @Description("BQ project ID to read from")
  @Default.String("bigquery-public-data")
  String getProjectId();
  void setProjectId(String projectId);

  @Description("BQ dateaset ID to read from")
  @Default.String("faa")
  String getDatasetName();
  void setDatasetName(String datasetName);

  @Description("BQ table ID to read from")
  @Default.String("us_airports")
  String getTableName();
  void setTableName(String tableName);
}