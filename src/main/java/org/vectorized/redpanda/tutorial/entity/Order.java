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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\n");
        builder.append("\tid: ").append(this.id).append("\n");
        builder.append("\tdescription: ").append(this.description).append("\n");
        builder.append("\tvalue: ").append(this.value).append("\n");
        if (this.orderStatus != null) {
            builder.append("\torderStatus: ").append(this.orderStatus.getDescription()).append("\n");
        } else {
            builder.append("\torderStatus: empty").append("\n");
        }
        builder.append("}");
        return builder.toString();
    }

}
