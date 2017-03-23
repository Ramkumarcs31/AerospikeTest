package com.c1x.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

import com.source.aerospike.Console;
import com.source.aerospike.Parameters;

/**
 * Created by ramkumarv on 23/03/17.
 */
public class LoadTest {

    Console console = new Console();

    private void runMultiBinTest(AerospikeClient client, Parameters params) throws Exception {
        Key key = new Key(params.namespace, params.set, "putgetkey");
        Bin bin1 = new Bin("bin1", "value1");
        Bin bin2 = new Bin("bin2", "value2");

        console.info("Put: namespace=%s set=%s key=%s bin1=%s value1=%s bin2=%s value2=%s",
                key.namespace, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);

        client.put(params.writePolicy, key, bin1, bin2);

        console.info("Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

        Record record = client.get(params.policy, key);

        if (record == null) {
            throw new Exception(String.format(
                    "Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
        }

        validateBin(key, bin1, record);
        validateBin(key, bin2, record);
    }

    private void validateBin(Key key, Bin bin, Record record) {
        Object received = record.getValue(bin.name);
        String expected = bin.value.toString();

        if (received != null && received.equals(expected)) {
            console.info("Bin matched: namespace=%s set=%s key=%s bin=%s value=%s generation=%d expiration=%d",
                    key.namespace, key.setName, key.userKey, bin.name, received, record.generation, record.expiration);
        }
        else {
            console.error("Put/Get mismatch: Expected %s. Received %s.", expected, received);
        }
    }
}
