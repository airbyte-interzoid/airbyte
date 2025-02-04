package io.airbyte.cdk.load.pipeline

import io.airbyte.cdk.load.message.DestinationRecordAirbyteValue
import io.airbyte.cdk.load.message.PartitionedQueue
import io.airbyte.cdk.load.message.PipelineEvent
import io.airbyte.cdk.load.message.QueueWriter
import io.airbyte.cdk.load.message.WithStream
import io.airbyte.cdk.load.task.internal.LoadPipelineStepTask
import io.airbyte.cdk.load.write.DirectLoader
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class DirectLoadPipelineStep<K: WithStream, S: DirectLoader>(
    val accumulator: DirectLoadRecordAccumulator<K, S>,
    @Named("recordQueue")
    val inputQueue: PartitionedQueue<PipelineEvent<K, DestinationRecordAirbyteValue>>,
    @Named("batchStateUpdateQueue") val batchQueue: QueueWriter<BatchUpdate>,
    @Value("\${airbyte.destination.core.record-batch-size-override:null}") val batchSizeOverride: Long? = null,
    @Value("\${airbyte.destination.core.load-pipeline.input-parts:1}")
    override val numWorkers: Int,
): LoadPipelineStep {
    override fun taskForPartition(partition: Int): LoadPipelineStepTask<*, *, *, *, *> {
        return LoadPipelineStepTask(
                    accumulator,
                    inputQueue.consume(partition),
                    batchUpdateQueue = batchQueue,
                    outputPartitioner = NoopPartitioner(),
                    null,
                    batchSizeOverride?.let { RecordCountFlushStrategy(it) },
                    partition
                )
    }
}
