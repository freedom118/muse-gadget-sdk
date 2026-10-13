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
 * Third-party text-to-speech via ByteDance Volcano Engine Seed-TTS 2.0
 * (Doubao), reached over the OpenSpeech unidirectional streaming WSS plan
 * gateway. Authentication uses an Ark API key (CONFIG_MUSE_DOUBAO_TTS_TOKEN);
 * the clock still must be synced over SNTP because the WSS TLS handshake
 * validates the server certificate.
 *
 * Because the local network's gateway may hijack UDP/53 with a fake-ip TUN
 * resolver, the OpenSpeech host is resolved over DNS-over-HTTPS (AliDNS) and
 * pinned into lwIP's local hostlist before connecting, so the TLS SNI/Host and
 * certificate CN/SAN all stay on the real hostname.
 *
 * A turn's reply is synthesised on a one-shot task; the MP3 it streams back is
 * handed to the chat session's existing decoder through a StreamBuffer, and a
 * small control queue reports the end of the audio. muse_doubao_tts_pump() runs on
 * the chat session task and moves buffered MP3 into the decoder.
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Raises the MP3/control channels. Once, at startup. */
void muse_doubao_tts_init(void);

/* True once the system clock has been set (SNTP), so the GEC hash is valid. */
bool muse_doubao_tts_clock_ready(void);

/*
 * Begins synthesising `text` for turn generation `gen`. Returns false if a turn
 * is already being spoken or the clock isn't synced. The call copies `text`.
 */
bool muse_doubao_tts_start(const char *text, uint32_t gen);

/* Drops any in-flight synthesis for a turn that has since finished. */
void muse_doubao_tts_cancel(uint32_t gen);

/*
 * Moves streamed MP3 into the decoder (on the chat session task). `feed` takes
 * a chunk of MP3; `ended` is called once when the synthesis finishes (ok) or
 * fails. Non-blocking.
 */
void muse_doubao_tts_pump(void (*feed)(const uint8_t *data, size_t len),
                        void (*ended)(uint32_t gen, bool ok));

#ifdef __cplusplus
}
#endif
