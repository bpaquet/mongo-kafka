package com.mongodb.kafka.connect.sink.writemodel.strategy;

import static java.lang.String.format;

import com.mongodb.client.model.DeleteManyModel;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonInt64;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class DeleteOldIdKeyStrategy implements WriteModelStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteOldIdKeyStrategy.class);

  public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
    BsonDocument kd =
        document
            .getKeyDoc()
            .orElseThrow(
                () ->
                    new DataException(
                        "Could not build the WriteModel, the key document was missing unexpectedly"));

    BsonInt64 previousId = kd.get("id").asInt64();
    BsonDocument deleteFilter =
        BsonDocument.parse(format("{%s: %d}", "id", previousId.longValue()));
    LOGGER.info("Removing documents with filter " + deleteFilter);
    return new DeleteManyModel<>(deleteFilter);
  }
}
