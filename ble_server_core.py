import asyncio
import logging
import sys
import struct
import json
import argparse
from mesh_proto_helpers import inspect_and_filter_packet
from ble_worker import BLEHardwareWorker

# --- CONSTANTS AND SIGNATURE ---
START1, START2 = 0x94, 0xC3
#MAX_PACKET_SIZE = 1024  # Increased to 1KB to handle rich map/metadata payloads
MAX_PACKET_SIZE = 512
# Hardcoded metadata for the project presentation
__app_name__ = "Meshtastic BLE-TCP Advanced Bridge"
__version__ = "3.0.0-Vanilla"
__author__ = "Gemini Fast 3.5 & IU4QTF Martino"

# Print authoritative banner immediately, independent of logging levels
print("=" * 55)
print(f"  {__app_name__} (v{__version__})")
print(f"  Author: {__author__}")
print("=" * 55)

logger = logging.getLogger("BLE_Server_Core")


# =====================================================================
#  UTILITY FUNCTIONS & STREAM FRAMING HELPERS
# =====================================================================

async def read_framed_protobuf(reader) -> bytes:
    """
    Reads from the TCP socket respecting the official framing (0x94 0xC3 + 2 Byte Length).
    Returns only the pure Protobuf payload bytes, or None if the stream breaks.
    """
    try:
        # 1. Stream alignment: seek for the START magic bytes sequence
        while True:
            b1 = await reader.read(1)
            if not b1: 
                return None
            if b1[0] == START1:
                b2 = await reader.read(1)
                if not b2: 
                    return None
                if b2[0] == START2:
                    break  # Stream synchronization locked

        # 2. Read payload length (2 bytes, Big Endian)
        len_bytes = await reader.read(2)
        if len(len_bytes) < 2: 
            return None
        length = struct.unpack(">H", len_bytes)[0]

        # 3. Sanity check based on proxy specification limits
        if length <= 0 or length > MAX_PACKET_SIZE:
            logger.warning(f"[FRAMING-ERR] Invalid or oversized packet length: {length} bytes. Discarding.")
            return None

        # 4. Read the Protobuf payload in chunks to avoid TCP fragmentation issues
        protobuf_payload = b""
        while len(protobuf_payload) < length:
            chunk = await reader.read(length - len(protobuf_payload))
            if not chunk: 
                return None
            protobuf_payload += chunk

        return protobuf_payload

    except Exception as e:
        logger.error(f"[FRAMING-CRIT] Error during TCP stream deframing: {e}")
        return None 


def wrap_protobuf_packet(protobuf_bytes: bytes) -> bytes:
    """Wraps raw protobuf bytes into the packet frame required by clients (0x94 0xC3 + LEN)."""
    length = len(protobuf_bytes)
    header = struct.pack(">BBH", START1, START2, length)
    return header + protobuf_bytes


# =====================================================================
#  CORE NETWORK GATEWAY MANAGEMENT CLASS
# =====================================================================

class BLEServerCore:
    def __init__(self, config):
        self.config = config
        self.mac_address = config["ble_mac_address"]
        self.port_clients = config["port_clients"]
        self.port_proxy = config["port_proxy"]
        
        # Dynamic active client tracking sets for async multiplexing
        self.client_writers = set()
        self.proxy_writers = set()
        
        # Instantiating the hardware worker with a decoupled dynamic callback hook
        self.worker = BLEHardwareWorker(self.mac_address, None)
        self.worker.packet_callback = self.broadcast_from_radio
        
        # State tracking for the infrastructure barrier orchestration
        self.servers_running = False
        self.server_clients = None
        self.server_proxy = None

    def broadcast_from_radio(self, from_radio_bytes: bytes):
        """
        HARDWARE CALLBACK: Triggered whenever the T-Echo pushes data via BLE.
        Duplicates and routes the binary stream onto the active TCP ports.
        """
        # Analyze the packet structure (Source_port=0 denotes incoming from Hardware)
        analysis = inspect_and_filter_packet(from_radio_bytes, source_port=0)
        
        if not analysis["valid"]:
            # Route raw service/UART data exclusively to the UI client
            self._write_to_stream_set(self.client_writers, from_radio_bytes, "UI-Raw")
            return

        # 1. Route to C# UI Client (Sees all regular mesh traffic)
        if analysis["to_client"]:
            self._write_to_stream_set(self.client_writers, from_radio_bytes, "UI")

        # 2. Conditional routing to MQTT Proxy (Honors Hop Limit and filtering)
        if analysis["to_mqtt"]:
            self._write_to_stream_set(self.proxy_writers, from_radio_bytes, "MQTT-Proxy")

    def _write_to_stream_set(self, writers_set, data, target_label):
        """Encapsulates and writes framed data bytes to a group of active sockets."""
        if not writers_set:
            return

        framed_data = wrap_protobuf_packet(data)
        disconnected = set()
        
        for writer in writers_set:
            try:
                writer.write(framed_data)
            except Exception as e:
                logger.debug(f"[CORE-TX-ERR] Failed delivery to {target_label}: {e}")
                disconnected.add(writer)
                
        # Clean up dead socket structures safely
        for writer in disconnected:
            writers_set.discard(writer)

    async def handle_client_connection(self, reader, writer):
        """Handles lifecycle for inbound socket connections on PORT_CLIENTS (C# UI)"""
        peer = writer.get_extra_info('peername')
        logger.info(f"[CORE-NET] New C#/UI client socket attached from: {peer}")
        self.client_writers.add(writer)
        
        try:
            while True:
                data = await read_framed_protobuf(reader)
                if data is None:
                    break # Remote end disconnected or frame corrupted
                
                # Forward validated protobuf straight to the hardware queue
                await self.worker.enqueue_uplink(data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"[CORE-CONN-ERR] Connection on client port {self.port_clients} interrupted: {e}")
        finally:
            logger.info(f"[CORE-NET] C#/UI client disconnected: {peer}")
            self.client_writers.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
            
    async def handle_proxy_connection(self, reader, writer):
        """Handles lifecycle for inbound socket connections on PORT_PROXY (MQTT Proxy)"""
        peer = writer.get_extra_info('peername')
        logger.info(f"[CORE-NET] MQTT Proxy connection accepted from: {peer}")
        self.proxy_writers.add(writer)
        
        try:
            while True:
                data = await read_framed_protobuf(reader)
                if data is None:
                    break

                # Validate inbound packets coming from MQTT before passing them down to RF
                analysis = inspect_and_filter_packet(data, source_port=self.port_proxy)
                if analysis["valid"] and not analysis["to_client"]:
                    await self.worker.enqueue_uplink(data)
                else:
                    logger.warning(f"[CORE-FILTER] Rejected packet from proxy port {self.port_proxy}: not eligible for RF.")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"[CORE-CONN-ERR] Connection on proxy port {self.port_proxy} interrupted: {e}")
        finally:
            logger.info(f"[CORE-NET] MQTT Proxy detached: {peer}")
            self.proxy_writers.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass

    async def stop_tcp_servers(self):
        """Closes downstream listeners and aggressively drops all connected clients."""
        if not self.servers_running:
            return
            
        logger.warning("[CORE-BARRIER] BLE Link down. Dismantling active network ports aggressively.")
        self.servers_running = False
        
        # 1. Close listener objects
        if self.server_clients:
            self.server_clients.close()
        if self.server_proxy:
            self.server_proxy.close()
            
        # 2. Purge and tear down active UI client sockets
        for writer in list(self.client_writers):
            try:
                writer.close()
            except:
                pass
        self.client_writers.clear()
        
        # 3. Purge and tear down active MQTT proxy sockets
        for writer in list(self.proxy_writers):
            try:
                writer.close()
            except:
                pass
        self.proxy_writers.clear()

        # Wait for proper port de-allocation by the OS kernel
        if self.server_clients:
            await self.server_clients.wait_closed()
            self.server_clients = None
        if self.server_proxy:
            await self.server_proxy.wait_closed()
            self.server_proxy = None
            
        logger.info("[CORE-BARRIER] Downstream TCP barrier established. Network sockets fully purged.")

    async def start_tcp_servers(self):
        """Instantiates listening sockets to allow incoming application traffic."""
        if self.servers_running:
            return
            
        try:
            self.server_clients = await asyncio.start_server(
                self.handle_client_connection, '0.0.0.0', self.port_clients, reuse_address=True
            )
            self.server_proxy = await asyncio.start_server(
                self.handle_proxy_connection, '0.0.0.0', self.port_proxy, reuse_address=True
            )
            
            self.servers_running = True
            logger.info(f"[CORE-INIT] UI Client listener deployed on port {self.port_clients}")
            logger.info(f"[CORE-INIT] MQTT Proxy listener deployed on port {self.port_proxy}")
        except Exception as e:
            logger.error(f"[CORE-NET-ERR] Critical failure initializing TCP infrastructure: {e}")

    async def dynamic_barrier_orchestrator(self):
        """Monitors BLE hardware state, controlling network socket availability dynamically."""
        while True:
            await asyncio.sleep(1)
            hw_ready = self.worker.radio_is_ready
            
            if hw_ready and not self.servers_running:
                logger.info("[ORCHESTRATOR] Hardware signaling ready. Re-deploying network ports.")
                await self.start_tcp_servers()
            elif not hw_ready and self.servers_running:
                logger.error("[ORCHESTRATOR] Hardware link lost. Initiating defensive network shutdown.")
                await self.stop_tcp_servers()

    async def main_loop(self):
        logger.info("[CORE-INIT] Initializing runtime background workers...")
        try:
            await asyncio.gather(
                self.dynamic_barrier_orchestrator(),
                self.worker.monitor_connection_loop(),
                self.worker._tx_process_loop(),
                self.worker.watchdog_task()
            )
        except asyncio.CancelledError:
            logger.info("[CORE-SHUTDOWN] Execution cancellation triggered.")
        except Exception as e:
            logger.critical(f"[CORE-FATAL] Core coordination loop collapsed: {e}")
        finally:
            # Shutdown phase: clean up everything before exiting python interpreter
            await self.stop_tcp_servers()
            await self.worker.close_interface()



# =====================================================================
#  RUNTIME BOOTSTRAPPER AND PARSER CONFIGURATION
# =====================================================================

def load_configuration() -> dict:
    """Loads configuration schema parameters from JSON, setting up baseline defaults."""
    default_config = {
        "ble_mac_address": "DE:28:61:4B:C9:C8",
        "port_clients": 4403,
        "port_proxy": 4404,
        "telemetry_interval_sec": 1800,
        "debug": False
    }
    
    try:
        with open("config.json", "r") as f:
            file_config = json.load(f)
            # Merge defaults with JSON keys found to avoid missing key crashes
            default_config.update(file_config)
            logger.info("[CONFIG] Configuration variables loaded successfully from config.json.")
    except FileNotFoundError:
        logger.warning("[CONFIG] config.json missing. Generating one with factory defaults.")
        with open("config.json", "w") as f:
            json.dump(default_config, f, indent=4)
    except Exception as e:
        logger.error(f"[CONFIG-ERR] Error processing config.json: {e}. Falling back to default structures.")
        
    return default_config

if __name__ == "__main__":
    # Setup command line parser interface for overriding parameters
    parser = argparse.ArgumentParser(description="Advanced Meshtastic BLE-TCP Gateway Core Engine.")
    parser.add_argument("--mac", type=str, help="Override targeted BLE hardware MAC Address.")
    parser.add_argument("--debug", action="store_true", help="Force runtime logging parameters to DEBUG mode.")
    args = parser.parse_args()

    # Load parameters configuration structure
    runtime_config = load_configuration()
    
    # Apply CLI runtime parameter overrides if requested
    if args.mac:
        runtime_config["ble_mac_address"] = args.mac
    if args.debug:
        runtime_config["debug"] = True

    # Adjust runtime logging infrastructure visibility levels
    log_level = logging.DEBUG if runtime_config["debug"] else logging.WARNING
    logging.basicConfig(level=log_level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", force=True)
    logger.setLevel(log_level)

    # Instantiate gateway server core runtime engine
    core = BLEServerCore(runtime_config)
    try:
        asyncio.run(core.main_loop())
    except KeyboardInterrupt:
        # This catch block prevents the internal library atexit from deadlocking 
        # because we cleanly handled resource release in the finally block above.
        logger.info("[CORE-SHUTDOWN] Stop execution signal caught. Clean exit accomplished. Out.")
        sys.exit(0)
