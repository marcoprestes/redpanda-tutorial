package org.vectorized.redpanda.tutorial.entity;


import lombok.Getter;
import lombok.Setter;
import org.vectorized.redpanda.tutorial.enums.OrderStatus;

@Getter
@Setter
public class Order {

    private Long id;
    private String description;
    private Double value;
    private OrderStatus orderStatus;

}
