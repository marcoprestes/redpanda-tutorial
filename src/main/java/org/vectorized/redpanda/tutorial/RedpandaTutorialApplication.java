package org.vectorized.redpanda.tutorial;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.vectorized.redpanda.tutorial.entity.Order;
import org.vectorized.redpanda.tutorial.enums.OrderStatus;
import org.vectorized.redpanda.tutorial.producer.Producer;

@SpringBootApplication
public class RedpandaTutorialApplication {


	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(RedpandaTutorialApplication.class, args);

		Producer producer = context.getBean(Producer.class);

		// Cancelled Order sample
		Order cancelledOrder = new Order();
		cancelledOrder.setId(123456L);
		cancelledOrder.setDescription("Cancelled order");
		cancelledOrder.setValue(253.45);
		cancelledOrder.setOrderStatus(OrderStatus.CANCELLED);
		producer.send(cancelledOrder);

		// Pending order
		Order pendingOrder = new Order();
		pendingOrder.setId(234567L);
		pendingOrder.setDescription("Pending order");
		pendingOrder.setValue(8456.25);
		pendingOrder.setOrderStatus(OrderStatus.PENDING);
		producer.send(pendingOrder);

		// New order
		Order newOrder = new Order();
		newOrder.setId(34567L);
		newOrder.setDescription("New order");
		newOrder.setValue(145.36);
		newOrder.setOrderStatus(OrderStatus.NEW);
		producer.send(newOrder);

		// On hold order
		Order onHoldOrder = new Order();
		onHoldOrder.setId(46578L);
		onHoldOrder.setDescription("On hold order");
		onHoldOrder.setValue(541.32);
		onHoldOrder.setOrderStatus(OrderStatus.ON_HOLD);
		producer.send(onHoldOrder);

		// Invalid order
		Order invalidOrder = new Order();
		invalidOrder.setId(56789L);
		invalidOrder.setDescription("Invalid order");
		invalidOrder.setValue(475.25);
		producer.send(invalidOrder);
	}

}
