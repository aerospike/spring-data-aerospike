/*
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.config;

import com.aerospike.client.Host;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import static org.springframework.data.aerospike.config.AerospikeDataConfigurationSupport.CONFIG_PREFIX_CONNECTION;

@Setter
@Getter
@ConfigurationProperties(prefix = CONFIG_PREFIX_CONNECTION)
public class AerospikeConnectionSettings {

    // String of hosts separated by ',' in form of hostname1[:tlsName1]:port1,...
    // An IP address must be given in one of the following formats:
    // IPv4: xxx.xxx.xxx.xxx
    // IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]
    // IPv6: [xxxx::xxxx]
    // IPv6 addresses must be enclosed by brackets. tlsName is optional.

    String hosts;
    // Storing hosts
    Host[] hostsArray;
}
