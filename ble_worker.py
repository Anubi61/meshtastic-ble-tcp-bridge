import asyncio
import logging
import time
import os
from meshtastic.ble_interface import BLEInterface
from meshtastic import mesh_pb2

logger = logging.getLogger("BLE_Worker")


class BLEHardwareWorker:
    def __init__(self, mac_address: str, config=None):
        """
        Manages the low-level Bluetooth Low Energy link layer connection
        to the Meshtastic hardware device (T-Echo) using asynchronous queues.
        """
        self.mac_address = mac_address
        self.config = config
        # Dynamic callback reference hooked by the orchestration server core
        self.packet_callback = None
        
        self.interface = None
        self.radio_is_ready = False
        self.last_hardware_packet_time = time.time()
        self.silence_threshold = 45  # Hardware watchdog timeout window in seconds
        
        # Asynchronous communication pipelines and concurrency locks
        self.tx_queue = asyncio.Queue()
        self.tx_semaphore = asyncio.Semaphore(1)  # Strict serialization for BLE physical writes
        self.loop = None

    def _radio_hardware_callback(self, from_radio_bytes):
        """
        Native BLE library callback trigger. Updates the hardware watchdog timestamp 
        and dispatches the raw byte stream to the orchestration core thread-safely.
        """
        self.last_hardware_packet_time = time.time()
        if self.loop and self.packet_callback:
            # Safely push the byte array back into the async loop execution thread
            self.loop.call_soon_threadsafe(self.packet_callback, from_radio_bytes)

    async def enqueue_uplink(self, raw_payload: bytes):
        """
        Invoked by downstream network clients (UI/Proxy) to queue 
        outbound payloads designated for RF transmission.
        """
        await self.tx_queue.put(raw_payload)
        logger.debug(f"[WORKER-QUEUE] Inbound TCP frame staged in TX queue. Size: {len(raw_payload)} bytes.")

    async def _tx_process_loop(self):
        """
        De-queues staged payloads sequentially and commits them to the physical BLE characteristic.
        Handles both standardized Protobuf envelopes and raw service/map UART structures natively.
        """
        logger.info("[WORKER-TX] Serialized transmission processing loop deployed.")
        while True:
            raw_payload = await self.tx_queue.get()
            
            if not self.radio_is_ready or not self.interface:
                logger.warning("[WORKER-TX-DROP] Local BLE interface offline. Outbound frame dropped.")
                self.tx_queue.task_done()
                continue

            async with self.tx_semaphore:
                try:
                    payload_to_send = None
                    # 1. Structural inspection attempt for internal logging visualization
                    try:
                        to_radio = mesh_pb2.ToRadio()
                        to_radio.ParseFromString(raw_payload)
                        p_id = to_radio.packet.id if to_radio.HasField("packet") else "Control/Config"
                        logger.info(f"[WORKER-TX] Transferring encoded Protobuf container (ID: {p_id}) to BLE layer.")
                        payload_to_send = to_radio
                    except Exception:
                        # 2. Fallback execution bypass: process as raw service/map payload directly
                        logger.info(f"[WORKER-TX] Transferring RAW/MAP service payload directly ({len(raw_payload)} bytes).")
                        payload_to_send = raw_payload

                    self.last_hardware_packet_time = time.time()
                    
                    # Offload the blocking hardware write operations onto an isolated IO worker thread
                    await asyncio.to_thread(self.interface._sendToRadio, payload_to_send)
                    
                    # Short structural cooling delay to prevent physical transceiver interface saturation
                    await asyncio.sleep(0.15)
                    
                except Exception as e:
                    logger.error(f"[WORKER-TX-ERR] Hardware layer reject during BLE transmission: {e}")
                finally:
                    self.tx_queue.task_done()

    async def close_interface(self):
        """Aggressive cleanup of the underlying library instance to prevent thread leaks."""
        if self.interface:
            logger.info("[WORKER-LINK] Cleaning up old hardware interface instances...")
            try:
                # Force close the internal synchronous interface thread
                await asyncio.to_thread(self.interface.close)
            except Exception as e:
                logger.debug(f"[WORKER-LINK-ERR] Error closing interface thread: {e}")
            finally:
                self.interface = None

    async def monitor_connection_loop(self):
        self.loop = asyncio.get_running_loop()
        logger.info("[WORKER-LINK] BLE Hardware supervisor routine initialized.")
        
        while True:
            if not self.radio_is_ready:
                # BEFORE reconnecting, explicitly destroy any corrupted or dead instance
                await self.close_interface()
                
                logger.info(f"[WORKER-LINK] Connecting to target hardware MAC address: [{self.mac_address}]...")
                try:
                    self.interface = await asyncio.to_thread(BLEInterface, self.mac_address)
                    self.interface._handleFromRadio = self._radio_hardware_callback
                    self.last_hardware_packet_time = time.time()
                    self.radio_is_ready = True
                    logger.info("[WORKER-LINK] BLE LINK CHANNEL ATTAINED AND FULLY OPERATIONAL.")
                except Exception as e:
                    logger.error(f"[WORKER-LINK-ERR] Connection attempt failed: {e}. Re-indexing queue in 20 seconds...")
                    self.radio_is_ready = False
                    await asyncio.sleep(20)
            await asyncio.sleep(5)


    async def watchdog_task(self):
        """
        Passive data flow supervisor. Detects internal library deadlocks
        or physical disconnection by tracking data arrival timestamps.
        Forces a process exit to let Systemd perform a clean initialization.
        """
        logger.info("[WORKER-WATCHDOG] Passive data flow supervisor initialized.")
        await asyncio.sleep(30)  # Initial stabilization window allowance
        
        while True:
            await asyncio.sleep(15)
            if self.radio_is_ready:
                silence_delta = time.time() - self.last_hardware_packet_time
                if silence_delta > self.silence_threshold:
                    # CRITICAL: We don't try to patch it. We die and let Systemd restart us.
                    logger.critical(
                        f"[WORKER-WATCHDOG] Hardware data starvation detected. "
                        f"Silent window: {int(silence_delta)}s. Forcing process exit for recovery."
                    )
                    
                    # Exit immediately. Python cached threads and ports are cleared by the OS kernel.
                    os._exit(1)
