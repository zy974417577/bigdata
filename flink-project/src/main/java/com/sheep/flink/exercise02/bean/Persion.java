package com.sheep.flink.exercise02.bean;

import lombok.Data;

@Data
public class Persion {

    public Persion() {}
    public Persion(String name,int age,String birthday) {
        this.age = age;
        this.name = name;
        this.birthday = birthday;
    }
    private String name;
    private int age;
    private String birthday;
}
