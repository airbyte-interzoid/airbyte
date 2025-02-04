/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_data_lake

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.MapperPipeline
import io.airbyte.cdk.load.data.iceberg.parquet.IcebergParquetPipelineFactory
import io.airbyte.cdk.load.message.DestinationRecordAirbyteValue
import io.airbyte.cdk.load.write.DirectLoader
import io.airbyte.cdk.load.write.DirectLoaderFactory
import io.airbyte.cdk.load.write.StreamStateStore
import io.airbyte.integrations.destination.s3_data_lake.io.S3DataLakeTableWriterFactory
import io.airbyte.integrations.destination.s3_data_lake.io.S3DataLakeUtil
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.data.Record
import org.apache.iceberg.io.BaseTaskWriter

@Singleton
class S3DataLakeDirectLoaderFactory(
    private val catalog: DestinationCatalog,
    private val config: S3DataLakeConfiguration,
    private val streamStateStore: StreamStateStore<S3DataLakeStreamState>,
    private val s3DataLakeTableWriterFactory: S3DataLakeTableWriterFactory,
    private val s3DataLakeUtil: S3DataLakeUtil,
) : DirectLoaderFactory<S3DataLakeDirectLoader>() {
    private val log = KotlinLogging.logger {}

    override fun create(streamDescriptor: DestinationStream.Descriptor, part: Int): S3DataLakeDirectLoader {
        log.info { "Creating direct loader for stream $streamDescriptor" }

        val state = streamStateStore.get(streamDescriptor)!!
        val stream = catalog.getStream(streamDescriptor)
        val writer = s3DataLakeTableWriterFactory.create(
            table = state.table,
            generationId = s3DataLakeUtil.constructGenerationIdSuffix(stream),
            importType = stream.importType,
            schema = state.schema
        )

        if (part > 0) {
            throw IllegalArgumentException(
                "Multiple partitions will not work properly without changes to how we finalize branches."
            )
        }

        return S3DataLakeDirectLoader(
            batchSize = config.recordBatchSizeBytes,
            stream = stream,
            table = state.table,
            schema = state.schema,
            stagingBranchName = DEFAULT_STAGING_BRANCH,
            writer = writer,
            s3DataLakeUtil = s3DataLakeUtil,
            pipeline = IcebergParquetPipelineFactory().create(stream)
        )
    }
}

class S3DataLakeDirectLoader(
    private val stream: DestinationStream,
    private val table: Table,
    private val schema: Schema,
    private val stagingBranchName: String,
    private val batchSize: Long,
    private val writer: BaseTaskWriter<Record>,
    private val s3DataLakeUtil: S3DataLakeUtil,
    private val pipeline: MapperPipeline
) : DirectLoader {
    private val log = KotlinLogging.logger {}
    private var dataSize = 0L

    override fun accept(record: DestinationRecordAirbyteValue): DirectLoader.DirectLoadResult {
        val icebergRecord =
            s3DataLakeUtil.toRecord(
                record = record,
                stream = stream,
                tableSchema = schema,
                pipeline = pipeline
            )
        writer.write(icebergRecord)

        dataSize += record.serializedSizeBytes // TODO: use icebergRecord.size() instead?
        if (dataSize < batchSize) {
            return DirectLoader.Incomplete
        }

        finish()

        return DirectLoader.Complete
    }

    override fun finish() {
        log.info { "Finishing writing to $stagingBranchName" }
        val writeResult = writer.complete()
        if (writeResult.deleteFiles().isNotEmpty()) {
            val delta = table.newRowDelta().toBranch(stagingBranchName)
            writeResult.dataFiles().forEach { delta.addRows(it) }
            writeResult.deleteFiles().forEach { delta.addDeletes(it) }
            delta.commit()
        } else {
            val append = table.newAppend().toBranch(stagingBranchName)
            writeResult.dataFiles().forEach { append.appendFile(it) }
            append.commit()
        }
        log.info { "Finished writing records to $stagingBranchName" }
    }

    override fun close() {
        log.info { "Closing writer for $stagingBranchName" }
        writer.close()
    }
}
