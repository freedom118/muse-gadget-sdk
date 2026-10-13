# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)
include("${CMAKE_CURRENT_LIST_DIR}/cmake/project.cmake")
project(muse-gadget)

# Doubao TTS resolves the OpenSpeech host over DoH and pins the result into
# lwIP's dynamic local hostlist (the local gateway hijacks UDP/53 with fake-ip).
# dns_local_addhost() is compiled inside lwIP and no Kconfig exposes these
# macros, so define them directly on the lwIP component target.
if(CONFIG_MUSE_DOUBAO_TTS)
    idf_build_get_property(build_components BUILD_COMPONENTS)
    if(lwip IN_LIST build_components)
        idf_component_get_property(lwip_target lwip COMPONENT_LIB)
        target_compile_definitions(${lwip_target} PUBLIC
            DNS_LOCAL_HOSTLIST=1
            DNS_LOCAL_HOSTLIST_IS_DYNAMIC=1)
    endif()
endif()

include("${CMAKE_CURRENT_LIST_DIR}/cmake/validate_config.cmake")
