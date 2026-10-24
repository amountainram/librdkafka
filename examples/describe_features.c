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
 * DescribeFeatures usage example: the features of the cluster (KIP-584),
 * e.g. the metadata.version it is finalized at, and the protocol API
 * versions of a broker.
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
                "Describe features usage examples\n"
                "\n"
                "Usage: %s <options> [<broker_id>]\n"
                "\n"
                "Describes the features of the cluster, and the API versions\n"
                "of the broker answering: the controller, or <broker_id>.\n"
                "\n"
                "Options:\n"
                "   -b <brokers>    Bootstrap server list to connect to.\n"
                "   -X <prop=val>   Set librdkafka configuration property.\n"
                "                   See CONFIGURATION.md for full list.\n"
                "   -d <dbg,..>     Enable librdkafka debugging (%s).\n"
                "   -a              Print the API versions too.\n"
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
int64_t parse_int(const char *what, const char *str) {
        char *end;
        long n = strtol(str, &end, 0);

        if (end != str + strlen(str)) {
                fprintf(stderr, "%% Invalid input for %s: %s: not an integer\n",
                        what, str);
                exit(1);
        }

        return (int64_t)n;
}


/**
 * @brief Print the \p cnt features of \p what.
 */
static void print_features(const char *what,
                           const rd_kafka_FeatureVersionRange_t **features,
                           size_t cnt) {
        size_t i;

        printf("%s features: %d\n", what, (int)cnt);
        for (i = 0; i < cnt; i++)
                printf("  %-36s %hd..%hd\n",
                       rd_kafka_FeatureVersionRange_name(features[i]),
                       rd_kafka_FeatureVersionRange_min_version(features[i]),
                       rd_kafka_FeatureVersionRange_max_version(features[i]));
}

/**
 * @brief Print the features, and the API versions if \p print_apis.
 */
static void print_result(const rd_kafka_DescribeFeatures_result_t *result,
                         int print_apis) {
        const rd_kafka_FeatureVersionRange_t **features;
        const rd_kafka_ApiVersionRange_t **apis;
        size_t cnt, i;

        printf(
            "Broker %" PRId32 ", finalized features epoch %" PRId64 "\n",
            rd_kafka_DescribeFeatures_result_node_id(result),
            rd_kafka_DescribeFeatures_result_finalized_features_epoch(result));

        features =
            rd_kafka_DescribeFeatures_result_finalized_features(result, &cnt);
        print_features("Finalized", features, cnt);
        features =
            rd_kafka_DescribeFeatures_result_supported_features(result, &cnt);
        print_features("Supported", features, cnt);

        apis = rd_kafka_DescribeFeatures_result_api_versions(result, &cnt);
        printf("APIs: %d\n", (int)cnt);
        if (!print_apis)
                return;
        for (i = 0; i < cnt; i++)
                printf("  %-32s (%3hd) %hd..%hd\n",
                       rd_kafka_ApiVersionRange_api_name(apis[i]),
                       rd_kafka_ApiVersionRange_api_key(apis[i]),
                       rd_kafka_ApiVersionRange_min_version(apis[i]),
                       rd_kafka_ApiVersionRange_max_version(apis[i]));
}


/**
 * @brief Call rd_kafka_DescribeFeatures()
 */
static void cmd_describe_features(rd_kafka_conf_t *conf,
                                  int argc,
                                  char **argv,
                                  int print_apis) {
        rd_kafka_t *rk = NULL;
        char errstr[512];
        rd_kafka_AdminOptions_t *options = NULL;
        rd_kafka_event_t *event          = NULL;
        int retval                       = 0;
        int32_t broker_id                = -1;

        if (argc > 1)
                usage("Wrong number of arguments.");
        if (argc == 1) {
                int64_t id = parse_int("broker_id", argv[0]);
                if (id < 0 || id > INT32_MAX)
                        usage("broker_id not a broker id");
                broker_id = (int32_t)id;
        }

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

        options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBEFEATURES);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 10 * 1000 /* 10s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                retval = 1;
                goto exit;
        }
        if (broker_id != -1 &&
            rd_kafka_AdminOptions_set_broker(options, broker_id, errstr,
                                             sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set broker: %s\n", errstr);
                retval = 1;
                goto exit;
        }

        /* Call DescribeFeatures. */
        rd_kafka_DescribeFeatures(rk, options, queue);

        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /* indefinitely but limited by
                                               * the request timeout set
                                               * above (10s) */);

        if (!event) {
                /* User hit Ctrl-C,
                 * see yield call in stop() signal handler */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                rd_kafka_resp_err_t err = rd_kafka_event_error(event);
                /* DescribeFeatures request failed */
                fprintf(stderr, "%% DescribeFeatures failed[%" PRId32 "]: %s\n",
                        err, rd_kafka_event_error_string(event));
                retval = 1;
        } else {
                /* DescribeFeatures request succeeded */
                print_result(rd_kafka_event_DescribeFeatures_result(event),
                             print_apis);
        }


exit:
        /* Cleanup. */
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
        int print_apis = 0;
        argv0          = argv[0];

        /*
         * Create Kafka client configuration place-holder
         */
        conf = rd_kafka_conf_new();

        /*
         * Parse common options
         */
        while ((opt = getopt(argc, argv, "b:X:d:a")) != -1) {
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

                case 'a':
                        print_apis = 1;
                        break;

                default:
                        usage("Unknown option %c", (char)opt);
                }
        }

        cmd_describe_features(conf, argc - optind, &argv[optind], print_apis);
        return 0;
}
