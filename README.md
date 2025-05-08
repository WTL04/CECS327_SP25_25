# End-to-End IoT Smart Home System

This repository implements a complete end-to-end IoT pipeline using virtual devices (Fridge 1, Fridge 2, Dishwasher), a cloud database, a caching server, and a TCP client.

---

## 📁 Project Structure
├── server.py # TCP server with metadata-driven cache <br/>
├── client.py # Interactive TCP client <br/>
├── requirements.txt # Python dependencies <br/>
└── README.md # This file

---

## 🛠 Prerequisites

- Dataniz Account
- **Python 3.10+**  
- **pip**  
- A PostgreSQL database
- **git**

---

## 1. Create Dataniz Pipeline
- Create one connection between Dataniz and NeonDB
- Create as many devices and sensors for each device as you desire

## 2. Boot up Google Cloud Compute Engine VMs
- They need to be able to talk to each other (set up TCP connection)

## 3. Download client.py and server.py and requirements.txt on your VMs
- Choose one to act as the client, and the other as the server

## 4. Run the server
```bash
python server.py
```
- Enter IP Address: 0.0.0.0
- Enter Port Number: Whatever you used to set up TCP connection (we used 12345)

The server will:
1. Load metadata → build sensor_map & name_map
2. Populate the last 3 hours of readings into an in-memory cache (device_data)
3. Start a background thread to pull new data every 30 s
4. Listen for client queries

## 5. Run the client
```bash
python client.py
```
- Enter Host IP Address: same as server’s IP
- Enter Port Number: same as server’s port <br/>
Use the menu to select:
- Average fridge moisture (past 3 hours)
- Total dishwasher water (past 1 hour)
- Highest electricity consumer (past 3 hours)
- Exit with 0.

**THAT'S IT!**
