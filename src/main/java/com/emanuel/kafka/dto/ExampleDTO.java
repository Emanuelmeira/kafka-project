package com.emanuel.kafka.dto;

public class ExampleDTO  {

    private String information;
    private Integer value;

    public String getInformation() {
        return information;
    }

    public void setInformation(String information) {
        this.information = information;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "ExampleDTO{" +
                "information='" + information + '\'' +
                ", value=" + value +
                '}';
    }
}
