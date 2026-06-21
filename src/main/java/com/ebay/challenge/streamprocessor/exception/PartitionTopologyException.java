package com.ebay.challenge.streamprocessor.exception;

/** Signals that Kafka topic partitioning no longer matches the processor topology. */
public class PartitionTopologyException extends IllegalStateException {
    public PartitionTopologyException(String message) {
        super(message);
    }

    public PartitionTopologyException(String message, Throwable cause) {
        super(message, cause);
    }
}
