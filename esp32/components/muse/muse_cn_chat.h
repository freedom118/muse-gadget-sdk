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
 * Domestic (mainland China) chat backend backed by Volcano Engine (火山引擎),
 * replacing Muse's cloud ASR + LLM while the existing Doubao Seed-TTS module
 * keeps speaking the replies.
 *
 * A voice turn is two calls:
 *   1. ASR: Doubao Seed-ASR 2.0 over the OpenSpeech plan gateway, a binary,
 *      gzip-framed WebSocket at
 *      wss://openspeech.bytedance.com/api/v3/plan/sauc/bigmodel_async with
 *      resource id volc.seedasr.sauc.duration. See muse_cn_asr_transcribe().
 *   2. LLM: the transcript goes to an OpenAI-compatible Ark Chat Completions
 *      call (https://<base>/chat/completions, Bearer <ark key>). A typed turn
 *      skips step 1 and sends its text straight to the LLM.
 *
 * Both run synchronously from the chat session task's point of view and block
 * until the result or an error. Like the Doubao TTS module, every host is
 * resolved over DNS-over-HTTPS (AliDNS) and pinned into lwIP's local hostlist,
 * because the local gateway may answer UDP/53 with fake-ip 198.18.x addresses.
 * SNTP must have synced the clock for TLS certificate validation.
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* True when the CN chat backend is compiled in and an Ark key is configured. */
bool muse_cn_chat_enabled(void);

/* True once the system clock has been set (SNTP), so TLS validates. */
bool muse_cn_chat_clock_ready(void);

/*
 * Transcribes recorded speech through Doubao Seed-ASR 2.0 (OpenSpeech plan
 * gateway, binary/gzip frames). `pcm` is 16 kHz mono signed-16 little-endian
 * samples, `pcm_bytes` its byte count (no WAV header). On success returns true
 * and writes the NUL-terminated transcript to `out`; on failure returns false
 * with a short reason in `err`. `cancelled` aborts a long recognition.
 */
bool muse_cn_asr_transcribe(const uint8_t *pcm, size_t pcm_bytes,
                            char *out, size_t out_cap,
                            char *err, size_t err_cap,
                            const volatile bool *cancelled);

/*
 * Runs one LLM turn for an already-known user message (`text`, either a typed
 * line or an ASR transcript) and returns the assistant's reply.
 *
 * On success returns true and writes a NUL-terminated UTF-8 reply into `out`
 * (cut to `out_cap - 1` bytes). On failure returns false and writes a short
 * English reason into `err` (sized `err_cap`) for the existing turn_fail()
 * path. Blocks for the whole request; call on the chat session task, never an
 * audio callback. `cancelled` aborts a long request.
 */
bool muse_cn_chat_run(const char *text, char *out, size_t out_cap,
                      char *err, size_t err_cap,
                      const volatile bool *cancelled);

/* The configured system prompt (robot persona), or a sensible default. */
const char *muse_cn_chat_system_prompt(void);

#ifdef __cplusplus
}
#endif
