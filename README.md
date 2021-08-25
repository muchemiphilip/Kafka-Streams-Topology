# Kafka-Streams-Topology

## A real-time Kafka Streams Topology implementation

### This is a real-time Kafka Stream Application that perfoms the following ETL Operation, The Invoice data is provided by a random Invoice Simulator that persists the data to a Kafka Topic.



1. Select Invoices where DeliveryType = "HOME-DELIVERY" and push them to the shipment service queue.

2. Select Invoices where CustomerType = "PRIME" and create a notification event for the Loyalty Management Service.

3. Select all Invoices,mask the personal information and create records for Trend Analytics. When the records are ready,persist them to Hadoop storage for batch Analytics.
