/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.demos.kafka.utils;

/**
 *
 * @author hassan
 */
public interface IKafkaConstants {
    public static String KAFKA_BROKERS = "localhost:9092";

    public static Integer MESSAGE_COUNT=10;

    public static String CLIENT_ID="client1";

    public static String TOPIC_NAME="demo";

    public static String GROUP_ID_CONFIG="consumerGroup1";

    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=3;

    public static String OFFSET_RESET_LATEST="latest";

    public static String OFFSET_RESET_EARLIER="earliest";

    public static Integer MAX_POLL_RECORDS=1;
}
