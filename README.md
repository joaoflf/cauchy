
&nbsp;
<div align="center">
    <img alt="Cauchy Logo" src="./assets/cauchy-logo.png" alt="Logo" width="250">
</div>
&nbsp;

<div align="center">
<img alt="python badge" src="https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10-blue">
<img alt="build badge" src="https://github.com/joaoflf/cauchy/actions/workflows/build.yml/badge.svg">
</br></br

Cauchy is a distributed key-value store (DKVS) built in Python, inspired by the concepts presented in [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) by Martin Kleppmann. 

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
* **Command Line Interface:** Interact with the store using a user-friendly command line interface for both the server and client.
* **Multiple Databases:** Supports the creation, deletion, selection, and listing of multiple databases.
* **Data Replication:** Ensures data availability and fault tolerance by replicating data across different nodes.
* **Replica Management:** Configurable roles for nodes including leader and follower for data replication.
* **Consistent Hashing:** Utilizes consistent hashing for data distribution.
* **REST API Interface:** Provides a REST API for interacting with the store.

&nbsp;

## ⚙️ Installation

You should have Python 3.8+ installed on your system.

```
python --version
```

Install Cauchy globally using pip:

 ```
pip install cauchy
 ```

&nbsp;

## ⌨️ Usage

### Server

Start the Cauchy server:

* As a leader:

    ```
    cauchy server --port 5000 --role leader
    ```

* As a follower:

    ```
    cauchy server --port 6000 --role follower --leader-host localhost --leader-port 5000
    ```

### Client

Use the client CLI to interact with the server:

* Connect to the server and select a database:

    ```
    cauchy connect --host localhost --port 5000 --username user --password pass --database mydb
    ```

* List available databases:

    ```
    cauchy list-databases
    ```

* Create a new database:

    ```
    cauchy create-database newdb
    ```

* Select a database:

    ```
    cauchy select-database newdb
    ```

* Delete a database:

    ```
    cauchy delete-database newdb
    ```

* Set a key-value pair:

    ```
    cauchy set foo bar
    ```

* Retrieve the value of a key:

    ```
    cauchy get foo
    ```

* Delete a key:

    ```
    cauchy delete foo
    ```

&nbsp;

## 🏈 Gameplan

* Implement LSMTree storage engine ✅
* Implement server scaffolding and interface 📥
* Build CLI 📥
  
&nbsp;

## 📑 License

Distributed under the MIT License. See `LICENSE` for more information.
