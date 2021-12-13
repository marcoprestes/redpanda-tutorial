package org.vectorized.redpanda.tutorial.enums;

public enum OrderStatus {

    NEW("New"),
    ON_HOLD("On hold"),
    PENDING("Pending"),
    COMPLETED("Completed"),
    CANCELLED("Cancelled");

    OrderStatus(String description) {
        this.description = description;
    }

    String description;

    public String getDescription() {
        return description;
    }

}
