package com.c1x.aerospike;

import com.aerospike.client.*;

import com.aerospike.client.policy.ClientPolicy;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

import com.aerospike.client.Log.Level;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

import com.source.aerospike.Parameters;
import com.c1x.aerospike.LoadTest;


/**
 * Created by ramkumarv on 22/03/17.
 */
public class TestMain {

    public static void main(String args[]) {

        try {
            Options options = new Options();
            options.addOption("h", "host", true,
                    "List of seed hosts in format:\n" +
                            "hostname1[:tlsname][:port1],...\n" +
                            "The tlsname is only used when connecting with a secure TLS enabled server. " +
                            "If the port is not specified, the default port is used.\n" +
                            "IPv6 addresses must be enclosed in square brackets.\n" +
                            "Default: localhost\n" +
                            "Examples:\n" +
                            "host1\n" +
                            "host1:3000,host2:3000\n" +
                            "192.168.1.10:cert1:3000,[2001::1111]:cert2:3000\n"
            );
            options.addOption("p", "port", true, "Server default port (default: 3000)");
            options.addOption("U", "user", true, "User name");
            options.addOption("P", "password", true, "Password");
            options.addOption("n", "namespace", true, "Namespace (default: test)");
            options.addOption("s", "set", true, "Set name. Use 'empty' for empty set (default: demoset)");
            options.addOption("tls", "tlsEnable", false, "Use TLS/SSL sockets");
            options.addOption("tp", "tlsProtocols", true,
                    "Allow TLS protocols\n" +
                            "Values:  SSLv3,TLSv1,TLSv1.1,TLSv1.2 separated by comma\n" +
                            "Default: TLSv1.2"
            );
            options.addOption("tlsCiphers", "tlsCipherSuite", true,
                    "Allow TLS cipher suites\n" +
                            "Values:  cipher names defined by JVM separated by comma\n" +
                            "Default: null (default cipher list provided by JVM)"
            );
            options.addOption("tr", "tlsRevoke", true,
                    "Revoke certificates identified by their serial number\n" +
                            "Values:  serial numbers separated by comma\n" +
                            "Default: null (Do not revoke certificates)"
            );
            options.addOption("te", "tlsEncryptOnly", false,
                    "Enable TLS encryption and disable TLS certificate validation"
            );
            options.addOption("g", "gui", false, "Invoke GUI to selectively run tests.");
            options.addOption("d", "debug", false, "Run in debug mode.");
            options.addOption("u", "usage", false, "Print usage.");

            CommandLineParser parser = new PosixParser();
            CommandLine cl = parser.parse(options, args, false);

            Parameters params = parseParameters(cl);

            ClientPolicy policy = new ClientPolicy();
            policy.user = params.user;
            policy.password = params.password;
            policy.tlsPolicy = params.tlsPolicy;

            params.policy = policy.readPolicyDefault;
            params.writePolicy = policy.writePolicyDefault;

            Host[] hosts = Host.parseHosts(params.host, params.port);

            AerospikeClient client = new AerospikeClient(policy, hosts);

            try {
                params.setServerSpecific(client);
                LoadTest loadTest = new LoadTest();
                loadTest.runMultiBinTest(client,params);
            } finally {
                client.close();
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

    }


    /**
     * Parse command line parameters.
     */
    private static Parameters parseParameters(CommandLine cl) throws Exception {
        String host = cl.getOptionValue("h", "127.0.0.1");
        String portString = cl.getOptionValue("p", "3000");
        int port = Integer.parseInt(portString);
        String namespace = cl.getOptionValue("n","test");
        String set = cl.getOptionValue("s", "demoset");

        if (set.equals("empty")) {
            set = "";
        }

        String user = cl.getOptionValue("U");
        String password = cl.getOptionValue("P");

        if (user != null && password == null) {
            System.err.println("Password unavailable");
        }

        TlsPolicy tlsPolicy = null;

        if (cl.hasOption("tls")) {
            tlsPolicy = new TlsPolicy();

            if (cl.hasOption("tp")) {
                String s = cl.getOptionValue("tp", "");
                tlsPolicy.protocols = s.split(",");
            }

            if (cl.hasOption("tlsCiphers")) {
                String s = cl.getOptionValue("tlsCiphers", "");
                tlsPolicy.ciphers = s.split(",");
            }

            if (cl.hasOption("tr")) {
                String s = cl.getOptionValue("tr", "");
                tlsPolicy.revokeCertificates = Util.toBigIntegerArray(s);
            }

            if (cl.hasOption("te")) {
                tlsPolicy.encryptOnly = true;
            }
        }
        return new Parameters(tlsPolicy, host, port, user, password, namespace, set);
    }

}
