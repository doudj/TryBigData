package com.doudou.lesson004.groupComparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: OrderBean
 * @Author: doudou
 * @Datetime: 2019/12/5
 * @Description: 获取每个订单中所有的商品金额最大的那个
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;

    @Override
    public int compareTo(OrderBean o) {
        int orderIdCompare = this.orderId.compareTo(o.orderId);
        if (orderIdCompare == 0) {
            int priceCompare = this.price.compareTo(o.price);
            return -priceCompare;
        } else {
            return orderIdCompare;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId='" + orderId + '\'' +
                ", price=" + price +
                '}';
    }
}
