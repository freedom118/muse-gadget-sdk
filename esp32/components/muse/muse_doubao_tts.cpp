/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Text-to-speech via ByteDance Doubao Seed-TTS 2.0 (OpenSpeech "plan" WebSocket).
 *
 * Endpoint (unidirectional streaming, one synthesis per connection):
 *   wss://openspeech.bytedance.com/api/v3/plan/tts/unidirectional/stream
 *
 * Handshake: query api_key=<ark key>, headers
 *   Authorization: Bearer;<ark key>
 *   X-Api-Resource-Id: seed-tts-2.0
 *   X-Api-Connect-Id: <32 hex chars, no dashes>
 *
 * Request frame (this plan gateway uses its own "V1" framing, no sequence):
 *   0x11 0x10 0x10 0x00 | BE32 payload_len | UTF-8 JSON
 *
 * Response frame:
 *   4-byte header (byte1 high nibble = message type: 0x9 JSON event,
 *   0xB audio, 0xF error; byte1 low nibble flags: bit 0x4 marks a
 *   connection/session id field), then:
 *     BE32 sequence
 *     if flags & 0x4: BE32 id_len | id bytes
 *     BE32 payload_len | payload
 *   Audio payloads are MP3 chunks; the terminal event is a JSON frame with
 *   sequence 152 and an empty JSON object.
 *
 * A turn's reply is synthesised on a one-shot task whose stack lives in
 * PSRAM; the MP3 is handed to the chat session's existing decoder through a
 * StreamBuffer, and a small control queue reports the end of the audio.
 * muse_doubao_tts_pump() runs on the chat session task and moves buffered MP3
 * into the decoder.
 */

#include "muse_doubao_tts.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>

#include "esp_crt_bundle.h"
#include "esp_log.h"
#include "esp_random.h"
#include "esp_timer.h"
#include "esp_websocket_client.h"
#include "freertos/FreeRTOS.h"
#include "freertos/stream_buffer.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "freertos/idf_additions.h"
#include "esp_heap_caps.h"
#include "esp_http_client.h"
#include "lwip/sockets.h"
#include "lwip/dns.h"
#include "lwip/ip_addr.h"

/* sdkconfig */
#include "sdkconfig.h"

static const char *TAG = "muse_tts";

#ifndef CONFIG_MUSE_DOUBAO_TTS
#define CONFIG_MUSE_DOUBAO_TTS 0
#endif
#ifndef CONFIG_MUSE_DOUBAO_TTS_TOKEN
#define CONFIG_MUSE_DOUBAO_TTS_TOKEN ""
#endif
#ifndef CONFIG_MUSE_DOUBAO_TTS_VOICE
#define CONFIG_MUSE_DOUBAO_TTS_VOICE "zh_female_vv_uranus_bigtts"
#endif

#define TTS_HOST "openspeech.bytedance.com"
#define TTS_PATH "/api/v3/plan/tts/unidirectional/stream"
#define TTS_RESOURCE_ID "seed-tts-2.0"
#define DEFAULT_VOICE CONFIG_MUSE_DOUBAO_TTS_VOICE

#define MP3_STREAM_BYTES (32 * 1024)
#define MSG_BYTES (8 * 1024)

/* Plan gateway frame message types (high nibble of frame byte 1). */
#define MT_FULL_SERVER 0x9
#define MT_AUDIO 0xB
#define MT_ERROR 0xF
#define FLAG_HAS_ID 0x4

/* Terminal synthesis event sequence (empty JSON body). */
#define SEQ_FINISHED 152

struct ctrl_msg_t {
    uint32_t gen;
    bool ok;
};

static StreamBufferHandle_t s_mp3;
static QueueHandle_t s_ctrl;
static SemaphoreHandle_t s_lock;
static std::atomic<bool> s_busy{false};
/* External turn generation, echoed back in the terminal control event. */
static std::atomic<uint32_t> s_gen{0};
/* Monotonic internal run id. start() mints a new one and cancel() retires the
 * current one, independently of externally supplied turn generations, so a
 * stale task can never confuse a cancel with a later turn's generation. */
static std::atomic<uint32_t> s_run{0};

struct sess_t {
    uint32_t gen;
    uint32_t run;
    int64_t last_audio_us;
    bool got_end;
    bool failed;
};

/* A 32-char hex id (no dashes), as the OpenSpeech gateway expects. */
static void new_hex_id(char id[33])
{
    uint32_t r[4] = {esp_random(), esp_random(), esp_random(), esp_random()};
    snprintf(id, 33, "%08x%08x%08x%08x", (unsigned)r[0], (unsigned)r[1],
             (unsigned)r[2], (unsigned)r[3]);
}

/* JSON-escape the reply text into `out`. */
static void json_escape(std::string &out, const char *s)
{
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        switch (c) {
        case '"': out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
            if (c < 0x20) {
                char buf[8];
                snprintf(buf, sizeof(buf), "\\u%04x", c);
                out += buf;
            } else {
                out += (char)c;   /* pass UTF-8 through */
            }
        }
    }
}

/* Builds the single synthesis request frame (no sequence field). */
static void build_task_frame(const char *connect_id, const char *text,
                             std::string &out)
{
    std::string body =
        "{\"app\":{\"appid\":\"\",\"token\":\"\",\"cluster\":\"volcano_tts\","
        "\"resource_id\":\"" TTS_RESOURCE_ID "\"},"
        "\"user\":{\"uid\":\"muse-gadget\"},"
        "\"namespace\":\"TTS\",\"event\":200,"
        "\"req_params\":{\"text\":\"";
    json_escape(body, text);
    body += "\",\"speaker\":\"";
    body += DEFAULT_VOICE;
    body += "\",\"audio_params\":{\"format\":\"mp3\",\"sample_rate\":24000,"
            "\"channel\":1}}}";

    uint32_t n = (uint32_t)body.size();
    char head[8] = {(char)0x11, (char)0x10, (char)0x10, (char)0x00,
                    (char)(n >> 24), (char)(n >> 16), (char)(n >> 8), (char)n};
    out.assign(head, 4);
    out.append(head + 4, 4);
    out += body;
    (void)connect_id;
}

/* Parses one server frame: 0xB audio, 0x9 events (seq 152 = done), 0xF error. */
static void on_frame(sess_t *st, const uint8_t *d, size_t len)
{
    if (len < 8) {
        return;
    }
    unsigned mtype = (d[1] >> 4) & 0xF;
    unsigned flags = d[1] & 0xF;
    size_t off = 4;
    int32_t seq = (int32_t)(((uint32_t)d[off] << 24) | ((uint32_t)d[off + 1] << 16) |
                           ((uint32_t)d[off + 2] << 8) | d[off + 3]);
    off += 4;
    if (flags & FLAG_HAS_ID) {
        if (off + 4 > len) {
            return;
        }
        uint32_t idlen = ((uint32_t)d[off] << 24) | ((uint32_t)d[off + 1] << 16) |
                         ((uint32_t)d[off + 2] << 8) | d[off + 3];
        off += 4;
        if (off + idlen + 4 > len) {
            return;
        }
        off += idlen;
    }
    if (off + 4 > len) {
        return;
    }
    uint32_t plen = ((uint32_t)d[off] << 24) | ((uint32_t)d[off + 1] << 16) |
                    ((uint32_t)d[off + 2] << 8) | d[off + 3];
    off += 4;
    if (off + plen > len) {
        plen = (uint32_t)(len - off);
    }
    const uint8_t *payload = d + off;

    if (mtype == MT_AUDIO) {
        if (s_run.load() != st->run || plen == 0) {
            return;
        }
        st->last_audio_us = esp_timer_get_time();
        size_t sent = 0;
        while (sent < plen) {
            size_t space = (size_t)xStreamBufferSpacesAvailable(s_mp3);
            if (space == 0) {
                vTaskDelay(pdMS_TO_TICKS(10));
                if (s_run.load() != st->run) {
                    return;
                }
                continue;
            }
            size_t chunk = plen - sent < space ? plen - sent : space;
            sent += xStreamBufferSend(s_mp3, payload + sent, chunk, 0);
        }
    } else if (mtype == MT_FULL_SERVER) {
        bool empty = plen == 2 && payload[0] == '{' && payload[1] == '}';
        if (seq == SEQ_FINISHED || empty) {
            st->got_end = true;
        }
    } else if (mtype == MT_ERROR) {
        std::string msg((const char *)payload, plen);
        ESP_LOGW(TAG, "service error: %s", msg.c_str());
        st->failed = true;
    }
}
static void ws_event(void *arg, esp_event_base_t, int32_t id, void *data)
{
    sess_t *st = static_cast<sess_t *>(arg);
    esp_websocket_event_data_t *e = static_cast<esp_websocket_event_data_t *>(data);
    switch (id) {
    case WEBSOCKET_EVENT_CONNECTED:
        ESP_LOGI(TAG, "connected");
        break;
    case WEBSOCKET_EVENT_DATA:
        if (e->op_code == 2 && e->data_ptr && e->data_len > 0 && e->payload_offset == 0) {
            on_frame(st, reinterpret_cast<const uint8_t *>(e->data_ptr), (size_t)e->data_len);
        }
        break;
    case WEBSOCKET_EVENT_ERROR:
        ESP_LOGW(TAG, "websocket error handshake=%d tls=%d flags=0x%x errno=%d",
                 e->error_handle.esp_ws_handshake_status_code,
                 e->error_handle.esp_tls_stack_err,
                 (unsigned)e->error_handle.esp_tls_cert_verify_flags,
                 e->error_handle.esp_transport_sock_errno);
        st->failed = true;
        break;
    case WEBSOCKET_EVENT_DISCONNECTED:
    case WEBSOCKET_EVENT_CLOSED:
        st->failed = st->failed || !st->got_end;
        break;
    default:
        break;
    }
}



/* Raw UDP A-record query to a public resolver, bypassing the system resolver
 * (a fake-ip/TUN gateway answers DHCP DNS with 198.18.x addresses). */
/* Skip one possibly-compressed DNS name at *po; advance past it. Returns the
 * number of name bytes written into `name` (NUL-terminated, dotted), or -1. */
static int dns_read_name(const uint8_t *buf, int rn, int pos, char *name, size_t ncap)
{
    size_t no = 0;
    int cur = pos;
    bool jumped = false;
    int after = pos;
    int guard = 0;
    while (cur < rn && guard++ < 32) {
        uint8_t l = buf[cur];
        if (l == 0) {
            if (!jumped) {
                after = cur + 1;
            }
            break;
        }
        if ((l & 0xC0) == 0xC0) {
            if (cur + 1 >= rn) {
                return -1;
            }
            if (!jumped) {
                after = cur + 2;
            }
            cur = ((l & 0x3F) << 8) | buf[cur + 1];
            jumped = true;
            continue;
        }
        cur++;
        if (cur + l > rn || no + l + 2 >= ncap) {
            return -1;
        }
        if (no) {
            name[no++] = '.';
        }
        memcpy(name + no, buf + cur, l);
        no += l;
        name[no] = '\0';
        cur += l;
    }
    (void)after;
    return (int)no;
}

/* Build a DNS A query for `host` in `q`. Returns wire length, or -1. */
static int dns_build_query(const char *host, uint8_t *q, size_t cap, uint16_t id)
{
    size_t n = 0;
    q[n++] = (uint8_t)(id >> 8);
    q[n++] = (uint8_t)id;
    q[n++] = 0x01; q[n++] = 0x00;
    q[n++] = 0x00; q[n++] = 0x01;
    q[n++] = 0x00; q[n++] = 0x00;
    q[n++] = 0x00; q[n++] = 0x00;
    q[n++] = 0x00; q[n++] = 0x00;
    const char *p = host;
    while (*p) {
        const char *dot = strchr(p, '.');
        size_t lab = dot ? (size_t)(dot - p) : strlen(p);
        if (lab == 0 || lab > 63 || n + 1 + lab + 5 > cap) {
            return -1;
        }
        q[n++] = (uint8_t)lab;
        memcpy(q + n, p, lab);
        n += lab;
        p += lab;
        if (dot) {
            p++;
        }
    }
    q[n++] = 0x00;
    q[n++] = 0x00; q[n++] = 0x01;   /* A */
    q[n++] = 0x00; q[n++] = 0x01;   /* IN */
    return (int)n;
}

/*
 * Resolve `host` to an IPv4 string via raw UDP queries to a public resolver,
 * bypassing the fake-ip/TUN system resolver. Follows CNAME chains with fresh
 * queries. Returns false on failure.
 */
struct doh_buf_t {
    uint8_t *data;
    size_t len;
    size_t cap;
};

static esp_err_t doh_event(esp_http_client_event_t *evt)
{
    if (evt->event_id == HTTP_EVENT_ON_DATA && evt->data && evt->data_len > 0) {
        doh_buf_t *b = static_cast<doh_buf_t *>(evt->user_data);
        if (b && b->len + (size_t)evt->data_len <= b->cap) {
            memcpy(b->data + b->len, evt->data, evt->data_len);
            b->len += (size_t)evt->data_len;
        }
    }
    return ESP_OK;
}

/* Parse the A records out of a DNS wire response into one IPv4 string. */
static bool dns_parse_a(const uint8_t *r, int rn, char out_ip[16])
{
    if (rn < 12 || (r[3] & 0x0F) != 0) {
        return false;
    }
    uint16_t ancount = ((uint16_t)r[6] << 8) | r[7];
    int o = 12;
    while (o < rn) {          /* skip question section */
        uint8_t l = r[o];
        if ((l & 0xC0) == 0xC0) { o += 2; break; }
        if (l == 0) { o++; break; }
        o += 1 + l;
    }
    o += 4;
    char owner[160];
    for (uint16_t i = 0; i < ancount && o + 10 <= rn; i++) {
        (void)dns_read_name(r, rn, o, owner, sizeof(owner));
        while (o < rn) {
            uint8_t l = r[o];
            if ((l & 0xC0) == 0xC0) { o += 2; break; }
            if (l == 0) { o++; break; }
            o += 1 + l;
        }
        uint16_t rtype = ((uint16_t)r[o] << 8) | r[o + 1];
        uint16_t rdlen = ((uint16_t)r[o + 8] << 8) | r[o + 9];
        o += 10;
        if (o + rdlen > rn) {
            return false;
        }
        if (rtype == 1 && rdlen == 4) {
            if (r[o] == 198 && (r[o + 1] == 18 || r[o + 1] == 19)) {
                o += rdlen;
                continue;   /* fake-ip from a transparent proxy */
            }
            snprintf(out_ip, 16, "%u.%u.%u.%u", r[o], r[o + 1], r[o + 2], r[o + 3]);
            return true;
        }
        o += rdlen;
    }
    return false;
}

/*
 * Resolve `host` to IPv4 via DNS-over-HTTPS (RFC 8484) to AliDNS by literal
 * IP. A local fake-ip/TUN gateway hijacks UDP/53, but it cannot answer a TLS
 * request to a public resolver on 443, so the real CDN address comes back.
 */
static bool raw_dns_resolve(const char *host, char out_ip[16])
{
    uint8_t query[300];
    int qn = dns_build_query(host, query, sizeof(query), 0xAA55);
    if (qn < 0) {
        return false;
    }
    uint8_t *resp = static_cast<uint8_t *>(heap_caps_malloc(1024, MALLOC_CAP_SPIRAM));
    if (!resp) {
        resp = static_cast<uint8_t *>(malloc(1024));
    }
    if (!resp) {
        return false;
    }
    doh_buf_t buf = {resp, 0, 1024};
    esp_http_client_config_t cfg = {};
    cfg.url = "https://223.5.5.5/dns-query";
    cfg.method = HTTP_METHOD_POST;
    cfg.crt_bundle_attach = esp_crt_bundle_attach;
    cfg.timeout_ms = 8000;
    cfg.buffer_size = 1024;
    cfg.user_data = &buf;
    cfg.event_handler = doh_event;
    cfg.disable_auto_redirect = true;

    bool ok = false;
    esp_http_client_handle_t c = esp_http_client_init(&cfg);
    if (c) {
        esp_http_client_set_header(c, "Content-Type", "application/dns-message");
        esp_http_client_set_header(c, "Accept", "application/dns-message");
        esp_http_client_set_post_field(c, (const char *)query, qn);
        esp_err_t e = esp_http_client_perform(c);
        int status = esp_http_client_get_status_code(c);
        if (e == ESP_OK && status == 200) {
            ok = dns_parse_a(resp, (int)buf.len, out_ip);
        } else {
            ESP_LOGW(TAG, "DoH failed e=%s status=%d bytes=%d", esp_err_to_name(e), status, (int)buf.len);
        }
        esp_http_client_cleanup(c);
    }
    free(resp);
    return ok;
}

static void tts_task(void *arg)
{
    char *text = static_cast<char *>(arg);
    sess_t st = {};
    st.gen = s_gen.load();
    st.run = s_run.load();

    char connect_id[33];
    new_hex_id(connect_id);

    const char *token = CONFIG_MUSE_DOUBAO_TTS_TOKEN;

    /* The system resolver is behind a fake-ip/TUN gateway (198.18.x). Resolve
     * the real CDN IPv4 over DoH and pin it into lwIP local hostlist, then
     * connect by hostname so TLS SNI/Host and certificate CN/SAN all match. */
    char real_ip[16] = {};
    if (raw_dns_resolve(TTS_HOST, real_ip)) {
        ESP_LOGI(TAG, "resolved %s -> %s (DoH)", TTS_HOST, real_ip);
        ip_addr_t pin = {};
        ipaddr_aton(real_ip, &pin);
        dns_local_removehost(TTS_HOST, &pin);
        dns_local_addhost(TTS_HOST, &pin);
    } else {
        ESP_LOGW(TAG, "DoH resolve failed; using system resolver");
    }

    char uri[320];
    snprintf(uri, sizeof(uri),
             "wss://%s%s?api_key=%s", TTS_HOST, TTS_PATH, token);

    char headers[420];
    snprintf(headers, sizeof(headers),
             "Authorization: Bearer;%s\r\n"
             "X-Api-Resource-Id: %s\r\n"
             "X-Api-Connect-Id: %s\r\n",
             token, TTS_RESOURCE_ID, connect_id);

    std::string frame;
    build_task_frame(connect_id, text, frame);

    esp_websocket_client_config_t cfg = {};
    cfg.uri = uri;
    cfg.headers = headers;
    cfg.transport = WEBSOCKET_TRANSPORT_OVER_SSL;
    cfg.crt_bundle_attach = esp_crt_bundle_attach;
    cfg.buffer_size = MSG_BYTES;
    cfg.task_stack = 6 * 1024;
    cfg.disable_auto_reconnect = true;
    cfg.disable_pingpong_discon = true;
    cfg.network_timeout_ms = 15000;
    cfg.user_context = &st;


    bool ok = false;
    esp_websocket_client_handle_t client = esp_websocket_client_init(&cfg);
    if (client) {
        esp_websocket_register_events(client, WEBSOCKET_EVENT_ANY, ws_event, &st);
        if (esp_websocket_client_start(client) == ESP_OK) {
            int64_t deadline = esp_timer_get_time() + 15 * 1000000LL;
            while (esp_timer_get_time() < deadline &&
                   !esp_websocket_client_is_connected(client) && !st.failed &&
                   s_run.load() == st.run) {
                vTaskDelay(pdMS_TO_TICKS(20));
            }
            if (esp_websocket_client_is_connected(client) && s_run.load() == st.run) {
                esp_websocket_client_send_bin(client, frame.data(), (int)frame.size(),
                                              pdMS_TO_TICKS(5000));
                /* Activity watchdog rather than a fixed total budget: long
                 * replies keep resetting it as audio frames stream in, so only
                 * a genuinely stalled connection times out (after 20 s idle),
                 * plus a hard 120 s ceiling as a final guard. */
                int64_t hard_deadline = esp_timer_get_time() + 120 * 1000000LL;
                st.last_audio_us = esp_timer_get_time();
                while (!st.got_end && !st.failed && s_run.load() == st.run &&
                       esp_timer_get_time() < hard_deadline &&
                       esp_timer_get_time() - st.last_audio_us < 20 * 1000000LL) {
                    vTaskDelay(pdMS_TO_TICKS(20));
                }
                vTaskDelay(pdMS_TO_TICKS(120));
                ok = st.got_end && !st.failed && s_run.load() == st.run;
            } else {
                ESP_LOGW(TAG, "did not connect");
            }
            esp_websocket_client_stop(client);
        }
        esp_websocket_client_destroy(client);
    } else {
        ESP_LOGW(TAG, "client init failed");
    }

    /* The task owns s_busy for its whole lifetime and always releases it on the
     * way out (there is exactly one task at a time), which lets a later start()
     * proceed once the old connection is fully torn down. The terminal control
     * event is reported only when this run is still the active one, so a
     * cancelled/retired turn can't end a newer turn's message. */
    if (s_run.load() == st.run) {
        ctrl_msg_t done = {st.gen, ok};
        xQueueSend(s_ctrl, &done, 0);
    }
    s_busy.store(false);
    free(text);
    vTaskDeleteWithCaps(xTaskGetCurrentTaskHandle());
}
extern "C" void muse_doubao_tts_init(void)
{
    if (s_mp3) {
        return;
    }
    s_mp3 = xStreamBufferCreateWithCaps(MP3_STREAM_BYTES, 1, MALLOC_CAP_SPIRAM);
    if (!s_mp3) {
        s_mp3 = xStreamBufferCreate(MP3_STREAM_BYTES, 1);
    }
    s_ctrl = xQueueCreateWithCaps(4, sizeof(ctrl_msg_t), MALLOC_CAP_SPIRAM);
    if (!s_ctrl) {
        s_ctrl = xQueueCreate(4, sizeof(ctrl_msg_t));
    }
    s_lock = xSemaphoreCreateMutex();
}

extern "C" bool muse_doubao_tts_clock_ready(void)
{
    /* Doubao does not hash the clock, but TLS certificate validation still
     * needs a roughly correct time, so require SNTP like the Edge backend did. */
    return time(nullptr) > 1700000000;
}

extern "C" bool muse_doubao_tts_start(const char *text, uint32_t gen)
{
#if CONFIG_MUSE_DOUBAO_TTS
    if (!text || !*text || !muse_doubao_tts_clock_ready() ||
        CONFIG_MUSE_DOUBAO_TTS_TOKEN[0] == '\0') {
        ESP_LOGW(TAG, "start refused (text=%d clock=%d token=%d)",
                 text && *text, muse_doubao_tts_clock_ready(),
                 CONFIG_MUSE_DOUBAO_TTS_TOKEN[0] != '\0');
        return false;
    }
    bool expected = false;
    if (!s_busy.compare_exchange_strong(expected, true)) {
        return false;
    }
    xStreamBufferReset(s_mp3);
    ctrl_msg_t drain;
    while (xQueueReceive(s_ctrl, &drain, 0) == pdTRUE) {
    }
    s_gen.store(gen);
    /* Mint a fresh internal run id so any task from a prior turn/cancel is
     * considered stale even if the external turn generation repeats. */
    s_run.fetch_add(1);

    char *copy = strdup(text);
    if (!copy) {
        s_busy.store(false);
        return false;
    }
    if (xTaskCreateWithCaps(tts_task, "doubao_tts", 8 * 1024, copy, 5, nullptr,
                            MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT) != pdPASS) {
        ESP_LOGW(TAG, "task create failed");
        free(copy);
        s_busy.store(false);
        return false;
    }
    ESP_LOGI(TAG, "speaking turn %u: %u chars", gen, (unsigned)strlen(text));
    return true;
#else
    (void)text;
    (void)gen;
    return false;
#endif
}

extern "C" void muse_doubao_tts_cancel(uint32_t gen)
{
    /* Retire the active run only when the caller refers to the current turn.
     * Bumping s_run makes a still-running task: (1) drop every further audio
     * frame (on_frame checks the run while filling the stream), (2) bail out of
     * its connect/receive wait loops promptly instead of sitting out the
     * timeout, and (3) suppress its terminal control event. The task keeps
     * owning s_busy until it unwinds and clears it itself, so a new turn can't
     * start until the old WebSocket has fully torn down. */
    if (s_gen.load() == gen) {
        s_run.fetch_add(1);
    }
    xStreamBufferReset(s_mp3);
}

extern "C" void muse_doubao_tts_pump(void (*feed)(const uint8_t *data, size_t len),
                                   void (*ended)(uint32_t gen, bool ok))
{
    if (!s_mp3) {
        return;
    }
    if (xSemaphoreTake(s_lock, 0) == pdTRUE) {
        static uint8_t chunk[1024];
        size_t n;
        while ((n = xStreamBufferReceive(s_mp3, chunk, sizeof(chunk), 0)) > 0) {
            feed(chunk, n);
        }
        xSemaphoreGive(s_lock);
    }
    ctrl_msg_t msg;
    if (xQueueReceive(s_ctrl, &msg, 0) == pdTRUE) {
        ended(msg.gen, msg.ok);
    }
}




















