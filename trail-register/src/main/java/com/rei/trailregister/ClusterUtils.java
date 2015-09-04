package com.rei.trailregister;

import static java.util.stream.Collectors.toList;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.net.HostAndPort;

class ClusterUtils {
    static final String PEERS_VAR = "PEERS";
    static final String PEERS_SEE_OTHER_VAR = "PEERS_ENV_VAR";
    
    static final String localhost = getLocalHost();
    
    static List<HostAndPort> parseHostAndPorts(Map<String, String> env) {
        String peersString = env.containsKey(PEERS_SEE_OTHER_VAR) ? env.get(env.get(PEERS_SEE_OTHER_VAR)) : env.get(PEERS_VAR);
        return Stream.of(peersString.split(",")).map(ClusterUtils::parseHostAndPort).collect(toList());
    }

    private static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "";
        }
    }

    private static HostAndPort parseHostAndPort(String input) {
        if (input.contains("[") && input.endsWith("]")) { // try jgroups format
            String host = input.substring(0, input.indexOf('['));
            int port = Integer.parseInt(input.substring(input.indexOf('[')+1, input.indexOf(']')));
            return HostAndPort.fromParts(host, port);
        }
        return HostAndPort.fromString(input);
    }
}
