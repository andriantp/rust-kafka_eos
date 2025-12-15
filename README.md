# Rust Kafka EOS + Avro Pipeline

This project is an implementation of a Rust-based Kafka pipeline, covering:
- Transactional Producer (Exactly-Once Semantics)
- EOS Consumer (read_committed)
- 3-topic pipeline: input → output → DLQ
- Avro encoding & Confluent wire format
- Schema Registry integration
- AKHQ UI

This pipeline can serve as a foundation for building robust, safe, and schema-evolution-resistant event-driven systems.

---

## 🚀 Getting Started

### Clone Repository
```bash
git clone ... rust-eos
cd rust-eos
```

---
### Start Kafka Stack (Zookeeper + Kafka + Schema Registry + AKHQ)

Enter the docker folder:
```bash
cd docker
```
Use the Makefile in the root directory:
```bash
make up
```

Active stack components:
- Kafka broker (PLAINTEXT, 9092)
- Schema Registry (8081)
- AKHQ UI (8080)
- Zookeeper (2181)
- Akses AKHQ:
    👉 [local](http://localhost:8080)


## 🔗 Reference

Full article:
[Rustifying My Kafka Pipeline: EOS, Transactions, and Schema Evolution](https://medium.com/@andriantriputra/be-rust-rustifying-my-kafka-pipeline-eos-transactions-and-schema-evolution-bb4f6b36c641)

---

## Author

Andrian Tri Putra
- [Medium](https://andriantriputra.medium.com/)
GitHub
- [andriantp](https://github.com/andriantp)
- [AndrianTriPutra](https://github.com/AndrianTriPutra)

---

## License
Licensed under the Apache License 2.0
