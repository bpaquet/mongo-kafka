package com.mongodb.kafka.connect.sink.namespace.mapping;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DATABASE_CONFIG;

import org.apache.kafka.connect.sink.SinkRecord;

import com.mongodb.MongoNamespace;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class TopicNamespaceMapper implements NamespaceMapper {

  private String defaultDatabaseName;

  @Override
  public void configure(MongoSinkTopicConfig config) {
    this.defaultDatabaseName = config.getString(DATABASE_CONFIG);
  }

  @Override
  public MongoNamespace getNamespace(SinkRecord sinkRecord, SinkDocument sinkDocument) {
    final String[] split = sinkRecord.topic().split("[.]");
    return new MongoNamespace(this.defaultDatabaseName, split[split.length - 1]);
  }
}
