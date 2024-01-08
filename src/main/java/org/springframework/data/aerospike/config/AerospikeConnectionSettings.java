package org.springframework.data.aerospike.config;

import com.aerospike.client.Host;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AerospikeConnectionSettings {

    // String of hosts separated by ',' in form of hostname1[:tlsName1]:port1,...
    // An IP address must be given in one of the following formats:
    // IPv4: xxx.xxx.xxx.xxx
    // IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]
    // IPv6: [xxxx::xxxx]
    // IPv6 addresses must be enclosed by brackets. tlsName is optional.
    String hosts;
    // Namespace
    String namespace;
    // Storing hosts
    Host[] hostsArray;
}
