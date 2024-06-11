// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package com.example;

import java.util.Arrays;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.google.api.services.bigquery.model.TableReference;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.schemas.Schema;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

public class App {
	public interface Options extends StreamingOptions {
		@Description("Input text to print.")
		@Default.String("My input text")
		String getInputText();

		void setInputText(String value);

		@Description("BQ project ID to read from")
		@Default.String("bigquery-public-data")
		String getProjectId();
		void setProjectId(String projectId);

		@Description("BQ dataset ID to read from")
		@Default.String("faa")
		String getDatasetName();
		void setDatasetName(String datasetName);

		@Description("BQ table ID to read from")
		@Default.String("us_airports")
		String getTableName();
		void setTableName(String tableName);

		@Description("GCS destination for the Iceberg data")
		// @Default.String("file:///Users/atognola/Documents/localCode/Spotify/Dataflow/Prueba/")
		@Default.String("gs://iceberg_test_bucket/")
		String getGcsDestination();
		void setGcsDestination(String gcsDestination);

		// @Description("GCS temp location to store temp files.")
		// @Validation.Required
		// String getTempLocation();
		// void setTempLocation(String tempLocation);
	}

	public static PCollection<String> buildPipeline(Pipeline pipeline, String inputText) {
		return pipeline
				.apply("Create elements", Create.of(Arrays.asList("Hello", "World!", inputText)))
				.apply("Print elements",
						MapElements.into(TypeDescriptors.strings()).via(x -> {
							System.out.println(x);
							return x;
						}));
	}

	public static void main(String[] args) {
		// Parse the pipeline options passed into the application. Example:
		// --runner=DirectRunner|DataflowRunner
		// --project=$PROJECT_ID
		// --stagingLocation=$DATAFLOW_STAGING_LOCATION
		// --region=$DATAFLOW_REGION
		// --gcsDestination=$ICEBERG_DESTINATION
		// For more information, see https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options
		PipelineOptionsFactory.register(Options.class);
		Options options = PipelineOptionsFactory.fromArgs(args)
				.withValidation()
				.as(Options.class);

		// Configure the Iceberg source I/O.
		Map catalogConfig = ImmutableMap.<String, Object>builder()
				.put("catalog_name", "local")
				.put("warehouse_location", options.getGcsDestination())
				.put("catalog_type", "hadoop")
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
						Schema.Field.of("faa_identifier", Schema.FieldType.STRING).withNullable(true),
						Schema.Field.of("name", Schema.FieldType.STRING)
				);

		// Create a pipeline and apply transforms.
		// options.setTempLocation("gs://temp-dataflow-atognola-test/");
		Pipeline pipeline = Pipeline.create(options);
		PCollection<TableRow> bq_rows =
				pipeline
						// Read table data into TableRow objects.
						.apply(BigQueryIO.readTableRows()
										.from(tableSpec)
										.withMethod(TypedRead.Method.DIRECT_READ)
								// .withRowRestriction("faa_identifier IS NOT NULL")
						)
				;

		PCollection<Row> beamRows = bq_rows.apply("tablerow2row",
				ParDo.of(new DoFn<TableRow, Row>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						TableRow tableRow = c.element();
						// Row beamRow = BigQueryUtils.toBeamRow(faa_schema, tableRow);
						// BigQueryUtils.
						System.out.print(tableRow.toString());

						Row r = Row.withSchema(faa_schema)
								.addValues(tableRow.get("faa_identifier"))
								.addValues(tableRow.get("name"))
								.build();
						c.output(r);
					}
				})
		);

		// beamRows.setRowSchema(faa_schema);
		beamRows.setCoder(RowCoder.of(faa_schema));
		PCollectionRowTuple output = PCollectionRowTuple.of("input", beamRows);

		PCollectionRowTuple write_out = output.apply(
				Managed.write(Managed.ICEBERG)
						.withConfig(config));

		pipeline.run().waitUntilFinish();
	}
}