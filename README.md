
&nbsp;
<div align="center">
    <img alt="Cauchy Logo" src="./assets/cauchy-logo.png" alt="Logo" width="250">
</div>
&nbsp;

<div align="center">

Cauchy is a scalable, lightweight, and robust distributed key-value store (DKVS) built in Python, inspired by the concepts presented in [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) by Martin Kleppmann. 

[📋 Overview](#-overview) •
[🎯 Features](#-features) •
[⚙️ Installation](#️-installation) •
[⌨️ Usage](#️-usage) •
[🏈 Gameplan](#-gameplan)

</div>

&nbsp;

## 📋 Overview

Cauchy aims to be an educational implementation of a distributed key-value store, complete with an LSMTree (Log-Structured Merge Tree) storage engine. The project is meant to learn about distributed data systems, data-intensive application design, and storage engines, providing an accessible codebase to learn from and experiment with.

&nbsp;

## 🎯 Features

* **Distributed:** Scales across multiple nodes.
* **LSMTree Storage Engine:** Leverages an LSMTree as storage engine for efficient data management.
* **Consistent Hashing:** Utilizes consistent hashing for data distribution.
* **Fault Tolerance:** Built-in resilience against node failures.
* **REST API Interface:** Provides a REST API for interacting with the store.

&nbsp;

## ⚙️ Installation

This project is setup as a python package.
```bash
# install from source
git clone git@github.com:joaoflf/cauchy.git
cd cauchy 
pip install .
```

## ⌨️ Usage

1. Start the Cauchy server:

    ```
    python cauchy/server.py
    ```

2. You can interact with the server using the REST API:

    * Set a key-value pair:

        ```
        curl -X POST http://localhost:5000/set -d '{"key":"foo", "value":"bar"}'
        ```

    * Retrieve the value of a key:

        ```
        curl -X GET http://localhost:5000/get/foo
        ```

    * Delete a key:

        ```
        curl -X DELETE http://localhost:5000/delete/foo
        ```

&nbsp;

## 🏈 Gameplan

* Implement basic HTTP server and CRUD operations scaffolding (single node) 🔄
* Implement LSMTree storage engine 📥
* ...
  
&nbsp;

## 📑 License

Distributed under the MIT License. See `LICENSE` for more information.
