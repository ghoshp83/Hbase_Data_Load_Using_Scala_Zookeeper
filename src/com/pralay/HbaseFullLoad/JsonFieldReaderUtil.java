package com.pralay.HbaseFullLoad;

import com.google.gson.JsonElement;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

public class JsonFieldReaderUtil {

    private static final Logger LOGGER = Logger.getLogger(JsonFieldReaderUtil.class);

    public static String getIcidValue(String fullJson) {

        String icid = "";

        try {
            JsonObject jobj = new Gson().fromJson(fullJson, JsonObject.class);
            if (jobj != null) {
                JsonObject header = jobj.getAsJsonObject("header");
                if (header != null) {
                    String valueToGet = header.get("icid").getAsString();
                    if (valueToGet != null) {
                        icid = valueToGet;
                    }
                }
            }
        } catch (JsonSyntaxException e) {
            LOGGER.warn("Malformed JSON record!", e);
            icid = "_";
        }

        return icid;
    }

    public static String getMoImsiValue(String fullJson) {
        String MT_STRING = "mo";
        return getImsiValue(fullJson, MT_STRING);
    }

    public static String getMtImsiValue(String fullJson) {
        String MT_STRING = "mt";
        return getImsiValue(fullJson, MT_STRING);
    }

    private static String getImsiValue(String fullJson, String legName) {
        String imsiValue = ""; // missing value
        try {
            JsonObject jobj = new Gson().fromJson(fullJson, JsonObject.class);
            if (jobj != null) {
                JsonObject dimensions = jobj.getAsJsonObject("dimensions");
                if (dimensions != null) {

                    JsonObject leg = dimensions.getAsJsonObject(legName);
                    if (leg != null) {
                        JsonElement imsi =  leg.get("imsi");
                        if( imsi != null) {
                            String valueToGet = imsi.getAsString();
                            if (valueToGet != null) {
                                imsiValue = valueToGet;
                            }
                        }
                    }
                }
            }
        } catch (JsonSyntaxException e) {
            LOGGER.warn("Malformed JSON record!", e);
            imsiValue = "_";// incorrect value
        }
        return imsiValue;
    }

    public static String getTsValue(String fullJson) {

        String ts = "";

        try {
            JsonObject jobj = new Gson().fromJson(fullJson, JsonObject.class);
            if (jobj != null) {
                JsonObject header = jobj.getAsJsonObject("header");
                if (header != null) {
                    String valueToGet = header.get("start").getAsString();
                    if (valueToGet != null) {
                        ts = valueToGet;
                    }
                }
            }

        } catch (JsonSyntaxException e) {
            LOGGER.warn("Malformed JSON record!", e);
            ts = "_";
        }

        return ts;
    }
}
