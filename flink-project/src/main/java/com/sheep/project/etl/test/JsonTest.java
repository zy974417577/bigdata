package com.sheep.project.etl.test;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.sheep.project.etl.bean.OrderFrom;

import java.util.List;

public class JsonTest {

    public static void main(String[] args) {

        String json = "{\"dt\":\"2018-01-01 10:11:11\",\"countryCode\":\"US\",\"orderData\":[{\"type\":\"s1\",\"score\":0.3,\"level\":\"A\"},{\"type\":\"s2\",\"score\":0.2,\"level\":\"B\"}]}";

        OrderFrom orderFrom = new Gson().fromJson(json,new TypeToken<OrderFrom>(){}.getType());


    }
}
