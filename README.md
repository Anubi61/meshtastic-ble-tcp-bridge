# Meshtastic BLE-TCP Advanced Bridge (v3.0.0-Vanilla)

An advanced, asynchronous Python gateway that bridges a Meshtastic hardware node (e.g., LilyGO T-Echo) via Bluetooth Low Energy (BLE) to multiplexed downstream TCP sockets. 

Developed as a collaborative project by **IU4QTF Martino** and **Gemini Fast 3.5**.

## Why This Project Exists (The Backstory)
This project was born out of local operational necessity and RF constraints. 

After acquiring a LilyGO T-Echo to test local Meshtastic coverage, it became apparent that local node density was extremely low, with no active mesh nodes in the immediate vicinity capable of relaying packets to the wider network. Maximizing antenna placement was critical. However, because the T-Echo relies exclusively on Bluetooth Low Energy (BLE) rather than Wi-Fi, it was tethered to short-range proximity.

To solve this, an unused Raspberry Pi 4 was deployed as an edge gateway. By creating this asynchronous BLE-to-TCP/IP bridge, the Meshtastic hardware could be dynamically positioned at strategic, elevated locations around the yard and garden to hunt for optimal RF propagation paths. Meanwhile, custom diagnostic clients (such as a native Windows UI) could interface with the radio safely from inside the local home network, monitors running continuously without signal loss.

Once the baseline TCP bridge achieved stable performance, the architecture naturally expanded to support an optional second step: integration with a downstream proxy to forward validated traffic to remote MQTT brokers. While the MQTT proxy layer provides worldwide connectivity, the core bridge alone successfully solved the local hardware isolation problem.

## Overview
This bridge allows multiple local and remote services to interact with a single Meshtastic BLE node simultaneously. It addresses the native library's single-connection limitation by acting as an asynchronous proxy middleware.

The downstream traffic targeted for the wider mesh network is seamlessly handled in cooperation with the **[mqtt-proxy by LN4CY](https://github.com/LN4CY/mqtt-proxy)** (or equivalent broker proxy interfaces), routing validated telemetry and text streams to remote central servers without saturating the local RF link.

### Key Features
* **Asynchronous Multiplexing:** Exposes independent TCP listeners for concurrent UI clients (e.g., custom applications) and MQTT proxies.
* **Dynamic Network Barrier:** Automatically spins up TCP ports when the BLE link is established and aggressively drops connected clients if the hardware goes offline, preventing stale socket configurations.
* **Fail-Fast Supervision:** Incorporates a low-level kernel-level watchdog (`os._exit(1)`) to forcefully mitigate BlueZ/Bleak thread deadlocks during physical disconnection, leveraging Systemd for instant clean recovery.
* **Intelligent Routing & Filtering:** Features a strict packet inspection matrix to manage telemetry rate-limiting and protect the RF mesh from forbidden downlinks.

## Project Structure
* `ble_server_core.py`: Main orchestration core, handles TCP framing (`0x94 0xC3`), stream decoding, configuration parsing, and network gateway synchronization.
* `ble_worker.py`: Manages physical BLE tx/rx queues, thread serialization via semaphores, and telemetry data starvation monitors.
* `mesh_proto_helpers.py`: Enforces allowlist validation profiles based on official Meshtastic Protobuf specifications.

## Configuration
1. Clone this repository into your local Linux environment (e.g., Raspberry Pi/DietPi).
2. Copy the configuration template:
   ```bash
   cp config.json.example config.json
3. Edit config.json and replace ble_mac_address with your specific device hardware MAC address.

## Production Deployment via Systemd
To ensure maximum availability and activate the automatic recovery engine, deploy the bridge as a native background service.

Create the service definition file /etc/systemd/system/meshtastic-ble.service:

[Unit]
Description=Meshtastic BLE-TCP Advanced Bridge
After=bluetooth.target network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/ble-tcp
ExecStart=/opt/ble-tcp/env/bin/python ble_server_core.py
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target

Reload systemd definitions, enable the service to launch at boot, and fire up the engine:


systemctl daemon-reload
systemctl enable meshtastic-ble.service
systemctl start meshtastic-ble.service

Monitor real-time logs and watchdog event executions:

journalctl -u meshtastic-ble.service -f

## License
This project is released under the terms of the MIT License.