# Thread clothing data pipeline

Event-driven AWS data pipeline for a simulated clothing brand in Vietnam and Singapore: two warehouses, five stores, market websites, marketplace stores and POS.

The project prioritizes normalized, traceable data across orders, payments, inventory, returns, purchases and marketplace settlements.

- [Implementation and local commands](clothing-pipeline/README.md)
- [AWS deployment and cost controls](clothing-pipeline/DEPLOYMENT.md)
- [AWS V2 Spark validation](clothing-pipeline/AWS-VALIDATION-V2.md)
- [Historical AWS V1 validation](clothing-pipeline/AWS-VALIDATION.md)
- [Design](clothing-data-pipeline-design-v1.md)

V2 uses PySpark, Iceberg and failure email monitoring. Its 24-event AWS smoke passed independent snapshot checks; retries kept 24 Bronze events and failure email delivery was confirmed. This does not establish production throughput. GitHub Actions runs unit/reference tests, Spark/Iceberg/DynamoDB integration tests, infrastructure validation and release builds. AWS deployment is manually dispatched and defaults to creating a change set. The Glue schedule stays disabled on deployment.
