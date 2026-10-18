/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * DescribeTopicPartitions usage example: describes the given topics, or all
 * of them, with their partitions' leader epoch, eligible leader replicas
 * and offline replicas (KIP-966).
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#ifdef _WIN32
#include "../win32/wingetopt.h"
#else
#include <getopt.h>
#endif


/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"


const char *argv0;
static rd_kafka_queue_t *queue = NULL; /** Admin result queue.
                                        *  This is a global so we can
                                        *  yield in stop() */
static volatile sig_atomic_t run = 1;

/**
 * @brief Signal termination of program
 */
static void stop(int sig) {
        if (!run) {
                fprintf(stderr, "%% Forced termination\n");
                exit(2);
        }
        run = 0;
        if (queue)
                rd_kafka_queue_yield(queue);
}


static void usage(const char *reason, ...) {

        fprintf(stderr,
                "Describe topic partitions usage examples\n"
                "\n"
                "Usage: %s <options> [<topic1> <topic2> ...]\n"
                "\n"
                "Describes the given topics, or all of them.\n"
                "\n"
                "Options:\n"
                "   -b <brokers>    Bootstrap server list to connect to.\n"
                "   -X <prop=val>   Set librdkafka configuration property.\n"
                "                   See CONFIGURATION.md for full list.\n"
                "   -d <dbg,..>     Enable librdkafka debugging (%s).\n"
                "   -l <limit>      Partitions per response: more take\n"
                "                   further requests (default 2000).\n"
                "   -a              Include the topics' authorized "
                "operations.\n"
                "\n",
                argv0, rd_kafka_get_debug_contexts());

        if (reason) {
                va_list ap;
                char reasonbuf[512];

                va_start(ap, reason);
                vsnprintf(reasonbuf, sizeof(reasonbuf), reason, ap);
                va_end(ap);

                fprintf(stderr, "ERROR: %s\n", reasonbuf);
        }

        exit(reason ? 1 : 0);
}


#define fatal(...)                                                             \
        do {                                                                   \
                fprintf(stderr, "ERROR: ");                                    \
                fprintf(stderr, __VA_ARGS__);                                  \
                fprintf(stderr, "\n");                                         \
                exit(2);                                                       \
        } while (0)


/**
 * @brief Set config property. Exit on failure.
 */
static void conf_set(rd_kafka_conf_t *conf, const char *name, const char *val) {
        char errstr[512];

        if (rd_kafka_conf_set(conf, name, val, errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK)
                fatal("Failed to set %s=%s: %s", name, val, errstr);
}


/**
 * @brief Parse an integer or fail.
 */
static int parse_int(const char *what, const char *str) {
        char *end;
        long n = strtol(str, &end, 0);

        if (end != str + strlen(str)) {
                fprintf(stderr, "%% Invalid input for %s: %s: not an integer\n",
                        what, str);
                exit(1);
        }

        return (int)n;
}

/**
 * @brief Print the ids of \p cnt \p nodes, as [1,2,3].
 */
static void
print_node_ids(const char *what, const rd_kafka_Node_t **nodes, size_t cnt) {
        size_t i;

        printf(", %s [", what);
        for (i = 0; i < cnt; i++)
                printf("%s%d", i ? "," : "", rd_kafka_Node_id(nodes[i]));
        printf("]");
}

/**
 * @brief Print partition information, on a line.
 */
static void
print_partition_info(const rd_kafka_TopicPartitionInfo_t *partition) {
        const rd_kafka_Node_t *leader =
            rd_kafka_TopicPartitionInfo_leader(partition);
        const rd_kafka_Node_t **nodes;
        size_t cnt;

        printf("\tpartition %d: ",
               rd_kafka_TopicPartitionInfo_partition(partition));
        if (leader)
                printf(
                    "leader %d (%s:%d, epoch %d)", rd_kafka_Node_id(leader),
                    rd_kafka_Node_host(leader) ? rd_kafka_Node_host(leader)
                                               : "?",
                    (int)rd_kafka_Node_port(leader),
                    (int)rd_kafka_TopicPartitionInfo_leader_epoch(partition));
        else
                printf("no leader");

        nodes = rd_kafka_TopicPartitionInfo_replicas(partition, &cnt);
        print_node_ids("replicas", nodes, cnt);
        nodes = rd_kafka_TopicPartitionInfo_isr(partition, &cnt);
        print_node_ids("isr", nodes, cnt);
        nodes = rd_kafka_TopicPartitionInfo_eligible_leader_replicas(partition,
                                                                     &cnt);
        print_node_ids("elr", nodes, cnt);
        nodes = rd_kafka_TopicPartitionInfo_last_known_eligible_leader_replicas(
            partition, &cnt);
        print_node_ids("last known elr", nodes, cnt);
        nodes = rd_kafka_TopicPartitionInfo_offline_replicas(partition, &cnt);
        print_node_ids("offline", nodes, cnt);
        printf("\n");
}

/**
 * @brief Print topic information.
 */
static void print_topic_info(const rd_kafka_TopicDescription_t *topic) {
        size_t j;
        const char *topic_name        = rd_kafka_TopicDescription_name(topic);
        const rd_kafka_error_t *error = rd_kafka_TopicDescription_error(topic);
        const rd_kafka_AclOperation_t *authorized_operations;
        size_t authorized_operations_cnt;
        const rd_kafka_TopicPartitionInfo_t **partitions;
        size_t partition_cnt;
        const char *topic_id_str =
            rd_kafka_Uuid_base64str(rd_kafka_TopicDescription_topic_id(topic));

        if (rd_kafka_error_code(error)) {
                printf("Topic: %s has error[%" PRId32 "]: %s\n", topic_name,
                       rd_kafka_error_code(error),
                       rd_kafka_error_string(error));
                return;
        }

        partitions =
            rd_kafka_TopicDescription_partitions(topic, &partition_cnt);
        printf("Topic: %s (Topic Id: %s)%s, %d partitions\n", topic_name,
               topic_id_str,
               rd_kafka_TopicDescription_is_internal(topic) ? ", internal" : "",
               (int)partition_cnt);

        authorized_operations = rd_kafka_TopicDescription_authorized_operations(
            topic, &authorized_operations_cnt);
        for (j = 0; j < authorized_operations_cnt; j++)
                printf("\t%s operation is allowed\n",
                       rd_kafka_AclOperation_name(authorized_operations[j]));

        for (j = 0; j < partition_cnt; j++)
                print_partition_info(partitions[j]);
}


/**
 * @brief Call rd_kafka_DescribeTopicPartitions() with a list of topics,
 *        none for all.
 */
static void cmd_describe_topic_partitions(rd_kafka_conf_t *conf,
                                          int limit,
                                          int include_authorized_operations,
                                          int argc,
                                          char **argv) {
        rd_kafka_t *rk                     = NULL;
        rd_kafka_TopicCollection_t *topics = NULL;
        char errstr[512];
        rd_kafka_AdminOptions_t *options = NULL;
        rd_kafka_event_t *event          = NULL;
        rd_kafka_error_t *error;
        int retval = 0;

        topics =
            rd_kafka_TopicCollection_of_topic_names((const char **)argv, argc);

        /*
         * Create producer instance
         * NOTE: rd_kafka_new() takes ownership of the conf object
         *       and the application must not reference it again after
         *       this call.
         */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk)
                fatal("Failed to create new producer: %s", errstr);

        queue = rd_kafka_queue_new(rk);

        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        options = rd_kafka_AdminOptions_new(
            rk, RD_KAFKA_ADMIN_OP_DESCRIBETOPICPARTITIONS);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s, all pages */, errstr,
                sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                retval = 1;
                goto exit;
        }
        if ((error = rd_kafka_AdminOptions_set_include_authorized_operations(
                 options, include_authorized_operations)) ||
            (error =
                 rd_kafka_AdminOptions_set_partition_size_limit_per_response(
                     options, limit))) {
                fprintf(stderr, "%% Failed to set options: %s\n",
                        rd_kafka_error_string(error));
                rd_kafka_error_destroy(error);
                retval = 1;
                goto exit;
        }

        /* Call DescribeTopicPartitions */
        rd_kafka_DescribeTopicPartitions(rk, topics, options, queue);

        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /* indefinitely but limited by
                                               * the request timeout set
                                               * above (30s) */);

        if (!event) {
                /* User hit Ctrl-C,
                 * see yield call in stop() signal handler */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                rd_kafka_resp_err_t err = rd_kafka_event_error(event);
                /* DescribeTopicPartitions request failed */
                fprintf(stderr,
                        "%% DescribeTopicPartitions failed[%" PRId32 "]: %s\n",
                        err, rd_kafka_event_error_string(event));
                retval = 1;

        } else {
                /* DescribeTopicPartitions request succeeded, but individual
                 * topics may have errors. */
                const rd_kafka_DescribeTopicPartitions_result_t *result =
                    rd_kafka_event_DescribeTopicPartitions_result(event);
                const rd_kafka_TopicDescription_t **result_topics;
                size_t result_topics_cnt;
                size_t i;

                result_topics = rd_kafka_DescribeTopicPartitions_result_topics(
                    result, &result_topics_cnt);
                printf("DescribeTopicPartitions results: %d topics\n",
                       (int)result_topics_cnt);
                for (i = 0; i < result_topics_cnt; i++)
                        print_topic_info(result_topics[i]);
        }


exit:
        /* Cleanup. */
        if (topics)
                rd_kafka_TopicCollection_destroy(topics);
        if (event)
                rd_kafka_event_destroy(event);
        if (options)
                rd_kafka_AdminOptions_destroy(options);
        if (queue)
                rd_kafka_queue_destroy(queue);
        if (rk)
                rd_kafka_destroy(rk);

        exit(retval);
}


int main(int argc, char **argv) {
        rd_kafka_conf_t *conf; /**< Client configuration object */
        int opt;
        int limit                         = 2000;
        int include_authorized_operations = 0;
        argv0                             = argv[0];

        /*
         * Create Kafka client configuration place-holder
         */
        conf = rd_kafka_conf_new();

        /*
         * Parse common options
         */
        while ((opt = getopt(argc, argv, "b:X:d:l:a")) != -1) {
                switch (opt) {
                case 'b':
                        conf_set(conf, "bootstrap.servers", optarg);
                        break;

                case 'X': {
                        char *name = optarg, *val;

                        if (!(val = strchr(name, '=')))
                                fatal("-X expects a name=value argument");

                        *val = '\0';
                        val++;

                        conf_set(conf, name, val);
                        break;
                }

                case 'd':
                        conf_set(conf, "debug", optarg);
                        break;

                case 'l':
                        limit = parse_int("limit", optarg);
                        break;

                case 'a':
                        include_authorized_operations = 1;
                        break;

                default:
                        usage("Unknown option %c", (char)opt);
                }
        }

        cmd_describe_topic_partitions(conf, limit,
                                      include_authorized_operations,
                                      argc - optind, &argv[optind]);
        return 0;
}
