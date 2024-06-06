import com.google.api.services.bigquery.model.TableReference;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.values.TypeDescriptor;

public class Iceberg_test {
  public static void main(String[] args) {
    // Parse the pipeline options passed into the application. Example:
    //   --projectId=$PROJECT_ID --datasetName=$DATASET_NAME --tableName=$TABLE_NAME
    // For more information, see https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options
    PipelineOptionsFactory.register(MyOptions.class);
    MyOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(MyOptions.class);

    // Configure the Iceberg source I/O.
    Map catalogConfig = ImmutableMap.<String, Object>builder()
        .put("catalog_name", "local")
        .put("warehouse_location", "gs://spotify-acct-team-test-dr-data-platform/")
        .put("catalog_type", "hive")
        .build();

    ImmutableMap<String, Object> config = ImmutableMap.<String, Object>builder()
        .put("table", "db.table1")
        .put("catalog_config", catalogConfig)
        .build();

    TableReference tableSpec =
        new TableReference()
            .setProjectId(options.getProjectId())
            .setDatasetId(options.getDatasetName())
            .setTableId(options.getTableName());

    Schema faa_schema =
        Schema.of(
            Schema.Field.of("faa_identifier", Schema.FieldType.STRING),
            Schema.Field.of("name", Schema.FieldType.STRING));

    // Create a pipeline and apply transforms.
    Pipeline pipeline = Pipeline.create(options);
    PCollection<TableRow> bq_rows =
        pipeline
            // Read table data into TableRow objects.
            .apply(BigQueryIO.readTableRows()
                .from(tableSpec)
                .withMethod(Method.DIRECT_READ)
                .withRowRestriction("faa_identifier IS NOT NULL")
            )
            //The output from the previous step is a PCollection<TableRow>.
            // .apply(MapElements
            //     .into(TypeDescriptor.of(TableRow.class))
            //     // Use TableRow to access individual fields in the row.
            //     .via((TableRow row) -> {
            //       System.out.print(row.toString());
            //       return row;
            //     })
            // )
        ;

    PCollection<Row> beamRows = bq_rows.apply("tablerow2row",
        ParDo.of(new DoFn<TableRow, Row>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            TableRow tableRow = c.element();
            Row beamRow = BigQueryUtils.toBeamRow(faa_schema, tableRow);
            System.out.print(beamRow.toString());
            c.output(beamRow);
          }
        })
    );

    beamRows.setRowSchema(faa_schema);
    PCollectionRowTuple output = PCollectionRowTuple.of("input", beamRows);

    PCollectionRowTuple write_out = output.apply(
        Managed.write(Managed.ICEBERG)
            .withConfig(config));

    pipeline.run().waitUntilFinish();
  }
}