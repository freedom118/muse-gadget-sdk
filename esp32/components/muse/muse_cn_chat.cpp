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
 * Volcano Engine (火山引擎) domestic chat backend. See muse_cn_chat.h.
 *
 * Two pieces:
 *   - muse_cn_asr_transcribe(): Doubao Seed-ASR 2.0 over the OpenSpeech plan
 *     gateway, a binary WebSocket with gzip-framed JSON config and PCM audio.
 *   - muse_cn_chat_run(): a non-streaming OpenAI-compatible Ark Chat
 *     Completions call for the LLM answer (typed text, or an ASR transcript).
 *
 * The short LLM JSON reply is buffered and parsed with cJSON. The ASR host and
 * Ark host are resolved over DNS-over-HTTPS and pinned into lwIP's local
 * hostlist, mirroring muse_doubao_tts.cpp, to defeat a local fake-ip/UDP-53
 * gateway. gzip uses the espressif__zlib managed component.
 */

#include "muse_cn_chat.h"

#include <ctime>
#include <cstdio>
#include <cstring>
#include <cstdlib>

#include "esp_crt_bundle.h"
#include "esp_heap_caps.h"
#include "esp_http_client.h"
#include "esp_log.h"
#include "esp_random.h"
#include "esp_timer.h"
#include "esp_websocket_client.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "lwip/dns.h"
#include "lwip/ip_addr.h"

#include "zlib.h"
#include "cJSON.h"
#include "sdkconfig.h"

#ifndef CONFIG_MUSE_CN_CHAT
#define CONFIG_MUSE_CN_CHAT 0
#endif
#ifndef CONFIG_MUSE_CN_ARK_TOKEN
#define CONFIG_MUSE_CN_ARK_TOKEN ""
#endif
#ifndef CONFIG_MUSE_CN_API_BASE
#define CONFIG_MUSE_CN_API_BASE "https://ark.cn-beijing.volces.com/api/plan/v3"
#endif
#ifndef CONFIG_MUSE_CN_TEXT_MODEL
#define CONFIG_MUSE_CN_TEXT_MODEL "ark-code-latest"
#endif
#ifndef CONFIG_MUSE_CN_ASR_RESOURCE_ID
#define CONFIG_MUSE_CN_ASR_RESOURCE_ID "volc.seedasr.sauc.duration"
#endif
#ifndef CONFIG_MUSE_CN_ASR_ENDPOINT
#define CONFIG_MUSE_CN_ASR_ENDPOINT "/api/v3/plan/sauc/bigmodel_async"
#endif
#ifndef CONFIG_MUSE_CN_SYSTEM_PROMPT
#define CONFIG_MUSE_CN_SYSTEM_PROMPT "你是一个友善的中文语音助手，用简洁口语化的中文回答。"
#endif
#ifndef CONFIG_MUSE_DOUBAO_TTS_TOKEN
#define CONFIG_MUSE_DOUBAO_TTS_TOKEN ""
#endif

static const char *TAG = "muse_cn";

#define RESP_CAP (24 * 1024)        /* a spoken reply's JSON is small */
#define HTTP_TIMEOUT_MS 20000
#define ASR_HOST "openspeech.bytedance.com"
/* 200 ms of 16 kHz mono s16le, the gateway's documented packet sweet spot. */
#define ASR_SEGMENT_BYTES (16000 * 2 * 200 / 1000)
#define ASR_RESULT_CAP (4 * 1024)

extern "C" bool muse_cn_chat_enabled(void)
{
#if CONFIG_MUSE_CN_CHAT
    return CONFIG_MUSE_CN_ARK_TOKEN[0] != '\0' || CONFIG_MUSE_DOUBAO_TTS_TOKEN[0] != '\0';
#else
    return false;
#endif
}

extern "C" bool muse_cn_chat_clock_ready(void)
{
    return time(nullptr) > 1700000000;
}

extern "C" const char *muse_cn_chat_system_prompt(void)
{
    return CONFIG_MUSE_CN_SYSTEM_PROMPT;
}

static const char *ark_token(void)
{
    return CONFIG_MUSE_CN_ARK_TOKEN[0] ? CONFIG_MUSE_CN_ARK_TOKEN : CONFIG_MUSE_DOUBAO_TTS_TOKEN;
}

/* ---- DNS-over-HTTPS (same fake-ip bypass as muse_doubao_tts.cpp) ---- */

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

static int dns_read_name(const uint8_t *buf, int rn, int pos, char *name, size_t ncap)
{
    size_t no = 0;
    int cur = pos;
    int guard = 0;
    while (cur < rn && guard++ < 32) {
        uint8_t l = buf[cur];
        if (l == 0) {
            break;
        }
        if ((l & 0xC0) == 0xC0) {
            cur = ((l & 0x3F) << 8) | buf[cur + 1];
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
    return (int)no;
}

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

/* Resolve `host` to IPv4 via DoH to AliDNS by literal IP (UDP/53 is hijacked). */
static bool doh_resolve(const char *host, char out_ip[16])
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
            ESP_LOGW(TAG, "DoH failed e=%s status=%d", esp_err_to_name(e), status);
        }
        esp_http_client_cleanup(c);
    }
    free(resp);
    return ok;
}

/* Splits a https://host/path base into its hostname and the request path. */
static bool split_base(const char *base, char *host, size_t hcap, char *path, size_t pcap)
{
    const char *p = base;
    if (strncmp(p, "https://", 8) != 0) {
        return false;
    }
    p += 8;
    const char *slash = strchr(p, '/');
    size_t hl = slash ? (size_t)(slash - p) : strlen(p);
    if (hl == 0 || hl >= hcap) {
        return false;
    }
    memcpy(host, p, hl);
    host[hl] = '\0';
    strlcpy(path, slash ? slash : "", pcap);
    return true;
}

/* ---- HTTP response buffering ---- */

struct resp_t {
    char *buf;
    size_t len;
    size_t cap;
};

static esp_err_t resp_event(esp_http_client_event_t *evt)
{
    if (evt->event_id == HTTP_EVENT_ON_DATA && evt->data && evt->data_len > 0) {
        resp_t *r = static_cast<resp_t *>(evt->user_data);
        size_t add = (size_t)evt->data_len;
        if (r->len + add + 1 > r->cap) {
            add = r->cap > r->len ? r->cap - r->len - 1 : 0;
        }
        if (add) {
            memcpy(r->buf + r->len, evt->data, add);
            r->len += add;
            r->buf[r->len] = '\0';
        }
    }
    return ESP_OK;
}

/* Builds the complete text-only LLM request body (system prompt + one user
 * message) with cJSON so the encoding is always valid. Returns a malloc'd
 * NUL-terminated buffer (caller frees), or nullptr on failure. */
static char *build_llm_body(const char *text)
{
    cJSON *root = cJSON_CreateObject();
    if (!root) {
        return nullptr;
    }
    cJSON_AddStringToObject(root, "model", CONFIG_MUSE_CN_TEXT_MODEL);
    cJSON_AddBoolToObject(root, "stream", false);

    cJSON *messages = cJSON_AddArrayToObject(root, "messages");
    cJSON *sys = cJSON_CreateObject();
    cJSON_AddStringToObject(sys, "role", "system");
    cJSON_AddStringToObject(sys, "content", muse_cn_chat_system_prompt());
    cJSON_AddItemToArray(messages, sys);

    cJSON *user = cJSON_CreateObject();
    cJSON_AddStringToObject(user, "role", "user");
    cJSON_AddStringToObject(user, "content", text ? text : "");
    cJSON_AddItemToArray(messages, user);

    char *body = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return body;
}

/* Pulls the first assistant message's text out of an Ark/OpenAI JSON response. */
static bool extract_reply(const char *json, char *out, size_t out_cap,
                          char *err, size_t err_cap)
{
    cJSON *root = cJSON_Parse(json);
    if (!root) {
        ESP_LOGW(TAG, "llm response not valid JSON, %u bytes", (unsigned)strlen(json));
        strlcpy(err, "BAD RESPONSE", err_cap);
        return false;
    }
    bool ok = false;
    cJSON *e = cJSON_GetObjectItem(root, "error");
    if (e) {
        cJSON *msg = cJSON_GetObjectItem(e, "message");
        snprintf(err, err_cap, "ARK: %s",
                 cJSON_IsString(msg) ? msg->valuestring : "ERROR");
        cJSON_Delete(root);
        return false;
    }
    cJSON *choices = cJSON_GetObjectItem(root, "choices");
    cJSON *first = cJSON_IsArray(choices) ? cJSON_GetArrayItem(choices, 0) : nullptr;
    cJSON *message = first ? cJSON_GetObjectItem(first, "message") : nullptr;
    cJSON *content = message ? cJSON_GetObjectItem(message, "content") : nullptr;
    if (cJSON_IsString(content) && content->valuestring && content->valuestring[0]) {
        strlcpy(out, content->valuestring, out_cap);
        ok = true;
    } else {
        strlcpy(err, "EMPTY REPLY", err_cap);
    }
    cJSON_Delete(root);
    return ok;
}

extern "C" bool muse_cn_chat_run(const char *text,
                                 char *out, size_t out_cap, char *err, size_t err_cap,
                                 const volatile bool *cancelled)
{
#if CONFIG_MUSE_CN_CHAT
    if (out_cap) {
        out[0] = '\0';
    }
    if (err_cap) {
        err[0] = '\0';
    }
    if (!muse_cn_chat_enabled()) {
        strlcpy(err, "NO ARK KEY", err_cap);
        return false;
    }
    if (!muse_cn_chat_clock_ready()) {
        strlcpy(err, "CLOCK NOT SYNCED", err_cap);
        return false;
    }
    if (!text || !text[0]) {
        strlcpy(err, "DIDN'T CATCH THAT", err_cap);
        return false;
    }

    char host[96], base_path[128];
    if (!split_base(CONFIG_MUSE_CN_API_BASE, host, sizeof(host), base_path, sizeof(base_path))) {
        strlcpy(err, "BAD API BASE", err_cap);
        return false;
    }
    (void)base_path;

    /* Pin the real CDN address over DoH: the local gateway's UDP/53 is fake-ip. */
    char real_ip[16] = {};
    if (doh_resolve(host, real_ip)) {
        ESP_LOGI(TAG, "resolved %s -> %s (DoH)", host, real_ip);
        ip_addr_t pin = {};
        ipaddr_aton(real_ip, &pin);
        dns_local_removehost(host, &pin);
        dns_local_addhost(host, &pin);
    } else {
        ESP_LOGW(TAG, "DoH resolve failed; using system resolver");
    }

    char url[256];
    snprintf(url, sizeof(url), "%s/chat/completions", CONFIG_MUSE_CN_API_BASE);

    char *rbuf = static_cast<char *>(heap_caps_malloc(RESP_CAP, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT));
    if (!rbuf) {
        rbuf = static_cast<char *>(malloc(RESP_CAP));
    }
    if (!rbuf) {
        strlcpy(err, "OUT OF MEMORY", err_cap);
        return false;
    }
    resp_t resp = {rbuf, 0, RESP_CAP};
    rbuf[0] = '\0';

    esp_http_client_config_t cfg = {};
    cfg.url = url;
    cfg.method = HTTP_METHOD_POST;
    cfg.crt_bundle_attach = esp_crt_bundle_attach;
    cfg.timeout_ms = HTTP_TIMEOUT_MS;
    cfg.buffer_size = 2048;
    cfg.buffer_size_tx = 2048;
    cfg.user_data = &resp;
    cfg.event_handler = resp_event;
    cfg.disable_auto_redirect = true;

    bool ok = false;
    esp_http_client_handle_t c = esp_http_client_init(&cfg);
    if (!c) {
        strlcpy(err, "HTTP INIT FAILED", err_cap);
        free(rbuf);
        return false;
    }
    char auth[160];
    snprintf(auth, sizeof(auth), "Bearer %s", ark_token());
    esp_http_client_set_header(c, "Content-Type", "application/json");
    esp_http_client_set_header(c, "Authorization", auth);
    esp_http_client_set_header(c, "Accept", "application/json");

    /* Build the full JSON body first, then send with an explicit
     * Content-Length (the plan gateway is validated against this form). */
    char *body = build_llm_body(text);
    if (!body) {
        strlcpy(err, "BODY BUILD FAILED", err_cap);
        esp_http_client_cleanup(c);
        free(rbuf);
        return false;
    }
    int body_len = (int)strlen(body);
    ESP_LOGI(TAG, "llm request: %d B", body_len);

    esp_err_t e = esp_http_client_open(c, body_len);
    if (e != ESP_OK) {
        snprintf(err, err_cap, "CONNECT FAILED (%s)", esp_err_to_name(e));
    } else if (esp_http_client_write(c, body, body_len) == body_len) {
        free(body);
        body = nullptr;
        int64_t deadline = esp_timer_get_time() + HTTP_TIMEOUT_MS * 1000LL;
        int64_t last_data = esp_timer_get_time();
        esp_http_client_fetch_headers(c);
        /* Read until the server closes the body or we have the full
         * Content-Length. A transient negative return (no data yet) must not
         * terminate the loop, or a fragmented JSON reply fails to parse. */
        int64_t content_len = esp_http_client_get_content_length(c);
        while (resp.len + 1 < resp.cap &&
               esp_timer_get_time() < deadline &&
               !(cancelled && *cancelled)) {
            if (content_len > 0 && (int64_t)resp.len >= content_len) {
                break;
            }
            char tmp[1024];
            int r = esp_http_client_read(c, tmp, sizeof(tmp));
            if (r > 0) {
                size_t add = (size_t)r;
                if (resp.len + add + 1 > resp.cap) {
                    add = resp.cap - resp.len - 1;
                }
                memcpy(resp.buf + resp.len, tmp, add);
                resp.len += add;
                resp.buf[resp.len] = '\0';
                last_data = esp_timer_get_time();
                if ((size_t)r > add) {
                    break;   /* reply larger than the buffer */
                }
            } else if (r == 0) {
                break;       /* clean end of body */
            } else if (esp_timer_get_time() - last_data > 5000000) {
                break;       /* 5 s idle: treat as a stalled connection */
            } else {
                vTaskDelay(pdMS_TO_TICKS(10));   /* recoverable: retry */
            }
        }
        int status = esp_http_client_get_status_code(c);
        if (cancelled && *cancelled) {
            strlcpy(err, "CANCELLED", err_cap);
        } else if (status == 200) {
            ok = extract_reply(resp.buf, out, out_cap, err, err_cap);
        } else {
            ESP_LOGW(TAG, "HTTP %d: %.*s", status, 200, resp.buf);
            snprintf(err, err_cap, "ARK HTTP %d", status);
        }
    }
    free(body);
    esp_http_client_cleanup(c);
    free(rbuf);

    if (ok) {
        ESP_LOGI(TAG, "turn ok: %u reply chars", (unsigned)strlen(out));
    } else if (!err[0]) {
        strlcpy(err, "ARK REQUEST FAILED", err_cap);
    }
    return ok;
#else
    (void)text; (void)out; (void)out_cap;
    (void)cancelled;
    strlcpy(err, "CN CHAT DISABLED", err_cap);
    return false;
#endif
}

/* ===================================================================== */
/* Doubao Seed-ASR 2.0 over the OpenSpeech plan gateway (binary frames). */
/* ===================================================================== */

/* v3 binary framing (see the VolcEngine speech protocol):
 *   byte0 = 0x11 (protocol v1, header size 1 -> 4 header bytes)
 *   byte1 = (message_type << 4) | flags
 *   byte2 = (serialization << 4) | compression
 *   byte3 = reserved
 * then BE32 sequence (when a sequence flag is set), BE32 payload size, payload.
 */
#define ASR_MSG_FULL_CLIENT   0x1
#define ASR_MSG_AUDIO_CLIENT  0x2
#define ASR_MSG_FULL_SERVER   0x9
#define ASR_MSG_ERROR         0xF
#define ASR_FLAG_SEQ_POS      0x1   /* positive sequence follows */
#define ASR_FLAG_SEQ_NEG      0x3   /* negative sequence: marks the last audio packet */
#define ASR_FLAG_SERVER_LAST  0x2
#define ASR_SER_NONE          0x0
#define ASR_SER_JSON          0x1
#define ASR_COMP_GZIP         0x1

/* gzip `in` (len) into a malloc'd buffer (caller frees). Returns size, 0 on error. */
static size_t gzip_bytes(const uint8_t *in, size_t len, uint8_t **out)
{
    z_stream zs = {};
    if (deflateInit2(&zs, Z_BEST_SPEED, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        return 0;
    }
    size_t cap = len + 64 + (len >> 5);
    uint8_t *buf = static_cast<uint8_t *>(heap_caps_malloc(cap, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT));
    if (!buf) {
        buf = static_cast<uint8_t *>(malloc(cap));
    }
    if (!buf) {
        deflateEnd(&zs);
        return 0;
    }
    zs.next_in = const_cast<Bytef *>(in);
    zs.avail_in = (uInt)len;
    zs.next_out = buf;
    zs.avail_out = (uInt)cap;
    int rc = deflate(&zs, Z_FINISH);
    size_t n = 0;
    if (rc == Z_STREAM_END) {
        n = zs.total_out;
    } else if (rc == Z_OK && zs.avail_out == 0) {
        /* Grew the buffer (unlikely for small PCM/config chunks). */
        size_t have = zs.total_out;
        size_t bigger = cap * 2;
        uint8_t *nb = static_cast<uint8_t *>(heap_caps_realloc(buf, bigger, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT));
        if (nb) {
            buf = nb;
            zs.next_out = buf + have;
            zs.avail_out = (uInt)(bigger - have);
            if (deflate(&zs, Z_FINISH) == Z_STREAM_END) {
                n = zs.total_out;
            }
        }
    }
    deflateEnd(&zs);
    if (!n) {
        free(buf);
        return 0;
    }
    *out = buf;
    return n;
}

static void put_be32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

/* Builds one frame into `frame` (cap). Returns wire length, 0 on overflow. */
static size_t build_frame(uint8_t msg_type, uint8_t flags, uint8_t serial, uint8_t comp,
                          int32_t seq, const uint8_t *payload, size_t plen,
                          uint8_t *frame, size_t cap)
{
    size_t n = 4;
    if (flags & 0x1) {
        n += 4;   /* sequence present (positive or negative, per flags) */
    }
    n += 4 + plen;
    if (n > cap) {
        return 0;
    }
    frame[0] = 0x11;
    frame[1] = (uint8_t)((msg_type << 4) | (flags & 0x0F));
    frame[2] = (uint8_t)((serial << 4) | (comp & 0x0F));
    frame[3] = 0x00;
    size_t o = 4;
    if (flags & 0x1) {
        put_be32(frame + o, (uint32_t)seq);
        o += 4;
    }
    put_be32(frame + o, (uint32_t)plen);
    o += 4;
    if (plen) {
        memcpy(frame + o, payload, plen);
    }
    return n;
}

/* gunzip `in` into a malloc'd NUL-terminated buffer (caller frees). 0 on error. */
static size_t gunzip_bytes(const uint8_t *in, size_t len, uint8_t **out, size_t cap)
{
    z_stream zs = {};
    if (inflateInit2(&zs, 15 + 16) != Z_OK) {
        return 0;
    }
    uint8_t *buf = static_cast<uint8_t *>(heap_caps_malloc(cap, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT));
    if (!buf) {
        buf = static_cast<uint8_t *>(malloc(cap));
    }
    if (!buf) {
        inflateEnd(&zs);
        return 0;
    }
    zs.next_in = const_cast<Bytef *>(in);
    zs.avail_in = (uInt)len;
    zs.next_out = buf;
    zs.avail_out = (uInt)(cap - 1);
    int rc = inflate(&zs, Z_FINISH);
    size_t n = 0;
    if (rc == Z_STREAM_END || rc == Z_OK || rc == Z_BUF_ERROR) {
        n = zs.total_out;
        buf[n] = '\0';
    }
    inflateEnd(&zs);
    if (!n) {
        free(buf);
        return 0;
    }
    *out = buf;
    return n;
}

struct asr_sess_t {
    bool got_last;      /* server flagged the final response */
    bool failed;
    char result[ASR_RESULT_CAP];
};

/* Parses one server frame; appends recognised text and notes the final flag. */
static void asr_on_frame(asr_sess_t *st, const uint8_t *d, size_t len)
{
    if (len < 4) {
        return;
    }
    size_t header_size = (d[0] & 0x0F) * 4;
    if (header_size < 4 || len < header_size) {
        return;
    }
    unsigned mtype = (d[1] >> 4) & 0x0F;
    unsigned flags = d[1] & 0x0F;
    unsigned serial = (d[2] >> 4) & 0x0F;
    unsigned comp = d[2] & 0x0F;
    size_t off = header_size;
    if (flags & ASR_FLAG_SEQ_POS) {
        off += 4;   /* server sequence */
        if (off > len) {
            return;
        }
    }

    if (mtype == ASR_MSG_ERROR) {
        if (off + 8 <= len) {
            int32_t code = (int32_t)(((uint32_t)d[off] << 24) | ((uint32_t)d[off + 1] << 16) |
                                     ((uint32_t)d[off + 2] << 8) | d[off + 3]);
            uint32_t mlen = ((uint32_t)d[off + 4] << 24) | ((uint32_t)d[off + 5] << 16) |
                            ((uint32_t)d[off + 6] << 8) | d[off + 7];
            ESP_LOGW(TAG, "asr error frame code=%d msglen=%u comp=%u", code, mlen, comp);
            size_t mo = off + 8;
            if (mlen && mo + mlen <= len) {
                uint8_t *msg = nullptr;
                size_t got = (comp == ASR_COMP_GZIP)
                                 ? gunzip_bytes(d + mo, mlen, &msg, ASR_RESULT_CAP)
                                 : 0;
                if (comp != ASR_COMP_GZIP) {
                    msg = static_cast<uint8_t *>(malloc(mlen + 1));
                    if (msg) {
                        memcpy(msg, d + mo, mlen);
                        msg[mlen] = '\0';
                        got = mlen;
                    }
                }
                if (msg && got) {
                    ESP_LOGW(TAG, "asr error message: %s", msg);
                }
                free(msg);
            }
        }
        st->failed = true;
        return;
    }
    if (mtype != ASR_MSG_FULL_SERVER || off + 4 > len) {
        return;
    }
    uint32_t plen = ((uint32_t)d[off] << 24) | ((uint32_t)d[off + 1] << 16) |
                    ((uint32_t)d[off + 2] << 8) | d[off + 3];
    off += 4;
    if (off + plen > len) {
        plen = (uint32_t)(len - off);
    }
    if (flags & ASR_FLAG_SERVER_LAST) {
        st->got_last = true;
    }
    if (!plen) {
        return;
    }
    uint8_t *json = nullptr;
    size_t jlen = plen;
    if (comp == ASR_COMP_GZIP) {
        jlen = gunzip_bytes(d + off, plen, &json, ASR_RESULT_CAP);
    } else {
        json = static_cast<uint8_t *>(malloc(plen + 1));
        if (json) {
            memcpy(json, d + off, plen);
            json[plen] = '\0';
        }
    }
    if (!json || !jlen) {
        free(json);
        return;
    }
    if (serial == ASR_SER_JSON) {
        cJSON *root = cJSON_Parse((const char *)json);
        if (root) {
            cJSON *result = cJSON_GetObjectItem(root, "result");
            if (result) {
                const char *text = cJSON_GetStringValue(cJSON_GetObjectItem(result, "text"));
                if (text && text[0]) {
                    strlcpy(st->result, text, sizeof(st->result));
                } else {
                    /* Concatenate utterance pieces when only that form is present. */
                    cJSON *utts = cJSON_GetObjectItem(result, "utterances");
                    if (cJSON_IsArray(utts) && !st->result[0]) {
                        cJSON *u;
                        cJSON_ArrayForEach(u, utts) {
                            const char *t = cJSON_GetStringValue(cJSON_GetObjectItem(u, "text"));
                            if (t) {
                                strlcat(st->result, t, sizeof(st->result));
                            }
                        }
                    }
                }
            }
            cJSON_Delete(root);
        }
    }
    free(json);
}

static void asr_ws_event(void *arg, esp_event_base_t, int32_t id, void *data)
{
    asr_sess_t *st = static_cast<asr_sess_t *>(arg);
    auto *e = static_cast<esp_websocket_event_data_t *>(data);
    switch (id) {
    case WEBSOCKET_EVENT_CONNECTED:
        ESP_LOGI(TAG, "asr connected");
        break;
    case WEBSOCKET_EVENT_DATA:
        if (e->op_code == 2 && e->data_ptr && e->data_len > 0 && e->payload_offset == 0) {
            asr_on_frame(st, reinterpret_cast<const uint8_t *>(e->data_ptr), (size_t)e->data_len);
        }
        break;
    case WEBSOCKET_EVENT_ERROR:
        ESP_LOGW(TAG, "asr ws error handshake=%d tls=%d",
                 e->error_handle.esp_ws_handshake_status_code, e->error_handle.esp_tls_stack_err);
        st->failed = true;
        break;
    case WEBSOCKET_EVENT_DISCONNECTED:
    case WEBSOCKET_EVENT_CLOSED:
        /* The final JSON normally arrives first; a close without it is a failure. */
        if (!st->got_last && !st->result[0]) {
            st->failed = true;
        }
        break;
    default:
        break;
    }
}

/* A 32-char hex id (no dashes), as the OpenSpeech gateway expects. */
static void asr_hex_id(char id[33])
{
    uint32_t r[4] = {esp_random(), esp_random(), esp_random(), esp_random()};
    snprintf(id, 33, "%08x%08x%08x%08x", (unsigned)r[0], (unsigned)r[1],
             (unsigned)r[2], (unsigned)r[3]);
}

extern "C" bool muse_cn_asr_transcribe(const uint8_t *pcm, size_t pcm_bytes,
                                       char *out, size_t out_cap,
                                       char *err, size_t err_cap,
                                       const volatile bool *cancelled)
{
#if CONFIG_MUSE_CN_CHAT
    if (out_cap) {
        out[0] = '\0';
    }
    if (err_cap) {
        err[0] = '\0';
    }
    if (!muse_cn_chat_enabled()) {
        strlcpy(err, "NO ARK KEY", err_cap);
        return false;
    }
    if (!muse_cn_chat_clock_ready()) {
        strlcpy(err, "CLOCK NOT SYNCED", err_cap);
        return false;
    }
    if (!pcm || pcm_bytes < 2) {
        strlcpy(err, "DIDN'T CATCH THAT", err_cap);
        return false;
    }

    /* Pin the OpenSpeech address over DoH (the local gateway's UDP/53 is fake-ip). */
    char real_ip[16] = {};
    if (doh_resolve(ASR_HOST, real_ip)) {
        ESP_LOGI(TAG, "asr resolved %s -> %s (DoH)", ASR_HOST, real_ip);
        ip_addr_t pin = {};
        ipaddr_aton(real_ip, &pin);
        dns_local_removehost(ASR_HOST, &pin);
        dns_local_addhost(ASR_HOST, &pin);
    } else {
        ESP_LOGW(TAG, "asr DoH resolve failed; using system resolver");
    }

    char connect_id[33];
    asr_hex_id(connect_id);

    char uri[200];
    snprintf(uri, sizeof(uri), "wss://%s%s", ASR_HOST, CONFIG_MUSE_CN_ASR_ENDPOINT);
    char headers[512];
    snprintf(headers, sizeof(headers),
             "X-Api-Key: %s\r\n"
             "X-Api-Resource-Id: %s\r\n"
             "X-Api-Connect-Id: %s\r\n"
             "X-Api-Request-Id: %s\r\n"
             "X-Api-Sequence: -1\r\n",
             ark_token(), CONFIG_MUSE_CN_ASR_RESOURCE_ID, connect_id, connect_id);

    asr_sess_t st = {};
    esp_websocket_client_config_t cfg = {};
    cfg.uri = uri;
    cfg.headers = headers;
    cfg.transport = WEBSOCKET_TRANSPORT_OVER_SSL;
    cfg.crt_bundle_attach = esp_crt_bundle_attach;
    cfg.buffer_size = 8 * 1024;
    cfg.task_stack = 6 * 1024;
    cfg.disable_auto_reconnect = true;
    cfg.disable_pingpong_discon = true;
    cfg.network_timeout_ms = 15000;
    cfg.user_context = &st;

    bool ok = false;
    esp_websocket_client_handle_t client = esp_websocket_client_init(&cfg);
    if (!client) {
        strlcpy(err, "ASR INIT FAILED", err_cap);
        return false;
    }
    esp_websocket_register_events(client, WEBSOCKET_EVENT_ANY, asr_ws_event, &st);

    if (esp_websocket_client_start(client) != ESP_OK) {
        strlcpy(err, "ASR START FAILED", err_cap);
        esp_websocket_client_destroy(client);
        return false;
    }

    int64_t deadline = esp_timer_get_time() + 15 * 1000000LL;
    while (esp_timer_get_time() < deadline &&
           !esp_websocket_client_is_connected(client) && !st.failed &&
           !(cancelled && *cancelled)) {
        vTaskDelay(pdMS_TO_TICKS(20));
    }
    if (!esp_websocket_client_is_connected(client) || st.failed ||
        (cancelled && *cancelled)) {
        if (cancelled && *cancelled) {
            strlcpy(err, "CANCELLED", err_cap);
        } else if (!err[0]) {
            strlcpy(err, "ASR CONNECT FAILED", err_cap);
        }
        esp_websocket_client_stop(client);
        esp_websocket_client_destroy(client);
        return false;
    }

    /* 1) Full client request: gzipped session JSON, sequence 1. */
    char cfg_json[512];
    snprintf(cfg_json, sizeof(cfg_json),
             "{\"user\":{\"uid\":\"muse-gadget\"},"
             "\"audio\":{\"format\":\"pcm\",\"codec\":\"raw\",\"rate\":16000,"
             "\"bits\":16,\"channel\":1},"
             "\"request\":{\"model_name\":\"bigmodel\",\"enable_itn\":true,"
             "\"enable_punc\":true,\"enable_ddc\":false,\"result_type\":\"full\"}}");
    uint8_t *gz = nullptr;
    size_t gz_len = gzip_bytes(reinterpret_cast<const uint8_t *>(cfg_json),
                               strlen(cfg_json), &gz);
    /* A frame holds a gzipped payload plus a 12-byte header. Audio segments
     * can be up to ASR_SEGMENT_BYTES (6400 B) of PCM, and gzip does not always
     * shrink speech, so size the buffer for the worst case rather than a fixed
     * 1100-byte stack slot (which silently overflowed build_frame). Prefer
     * PSRAM to avoid pressure on the small internal heap. */
    size_t frame_cap = ASR_SEGMENT_BYTES + 64;
    uint8_t *frame = static_cast<uint8_t *>(
        heap_caps_malloc(frame_cap, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT));
    if (!frame) {
        frame = static_cast<uint8_t *>(malloc(frame_cap));
    }
    if (!frame) {
        strlcpy(err, "ASR OOM", err_cap);
        esp_websocket_client_stop(client);
        esp_websocket_client_destroy(client);
        return false;
    }

    int64_t t0 = esp_timer_get_time();
    if (gz_len) {
        size_t flen = build_frame(ASR_MSG_FULL_CLIENT, ASR_FLAG_SEQ_POS,
                                  ASR_SER_JSON, ASR_COMP_GZIP, 1,
                                  gz, gz_len, frame, frame_cap);
        free(gz);
        int sent = flen ? esp_websocket_client_send_bin(client, (const char *)frame,
                                                        (int)flen, pdMS_TO_TICKS(5000))
                        : -1;
        ESP_LOGI(TAG, "asr full request sent=%d flen=%u at=%lldms", sent, (unsigned)flen,
                 (long long)((esp_timer_get_time() - t0) / 1000));
    }

    /* 2) Audio-only packets, ~200 ms each, gzipped, positive sequences. */
    int32_t seq = 2;
    size_t off = 0;
    int audio_pkts = 0;
    while (off < pcm_bytes && !st.failed && !(cancelled && *cancelled)) {
        size_t n = pcm_bytes - off;
        bool last = false;
        if (n > ASR_SEGMENT_BYTES) {
            n = ASR_SEGMENT_BYTES;
        } else {
            last = true;
        }
        uint8_t *agz = nullptr;
        size_t agz_len = gzip_bytes(pcm + off, n, &agz);
        off += n;
        if (!agz_len) {
            strlcpy(err, "ASR GZIP FAILED", err_cap);
            break;
        }
        int32_t wire_seq = last ? -seq : seq;
        uint8_t flags = last ? ASR_FLAG_SEQ_NEG : ASR_FLAG_SEQ_POS;
        size_t flen = build_frame(ASR_MSG_AUDIO_CLIENT, flags,
                                  ASR_SER_NONE, ASR_COMP_GZIP, wire_seq,
                                  agz, agz_len, frame, frame_cap);
        free(agz);
        if (!flen || esp_websocket_client_send_bin(client, (const char *)frame,
                                                    (int)flen,
                                                    pdMS_TO_TICKS(5000)) < 0) {
            strlcpy(err, "ASR SEND FAILED", err_cap);
            break;
        }
        audio_pkts++;
        if (audio_pkts == 1 || last) {
            ESP_LOGI(TAG, "asr audio: %d packet(s), last=%d", audio_pkts, (int)last);
        }
        seq++;
        vTaskDelay(pdMS_TO_TICKS(20));   /* pace like a live mic */
    }

    /* 3) Wait for the final result (hard 30 s ceiling). */
    int64_t hard = esp_timer_get_time() + 30 * 1000000LL;
    while (esp_timer_get_time() < hard && !st.failed &&
           !(cancelled && *cancelled) && !st.got_last) {
        vTaskDelay(pdMS_TO_TICKS(20));
    }
    if (cancelled && *cancelled) {
        strlcpy(err, "CANCELLED", err_cap);
    } else if (st.result[0]) {
        strlcpy(out, st.result, out_cap);
        ok = true;
        ESP_LOGI(TAG, "asr transcript: %u chars", (unsigned)strlen(out));
    } else if (!err[0]) {
        strlcpy(err, st.failed ? "ASR FAILED" : "ASR TIMEOUT", err_cap);
    }
    free(frame);
    esp_websocket_client_stop(client);
    esp_websocket_client_destroy(client);
    return ok;
#else
    (void)pcm; (void)pcm_bytes; (void)out; (void)out_cap;
    (void)cancelled;
    strlcpy(err, "CN CHAT DISABLED", err_cap);
    return false;
#endif
}
