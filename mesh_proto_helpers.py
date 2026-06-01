import logging
import time
from meshtastic import mesh_pb2
from meshtastic import portnums_pb2

logger = logging.getLogger("MeshProtoHelper")

# Internal runtime tracking for the telemetry rate-limiter window
_last_telemetry_mqtt_time = 0.0

# =====================================================================
#  OFFICIAL ALLOWLISTS AND ROUTING CONTROL MATRICES
# =====================================================================

# Public broker optimized priority list — our standard uplink allowlist
PUBLIC_UPLINK_PORTNUMS = {
    portnums_pb2.PortNum.NODEINFO_APP,
    portnums_pb2.PortNum.TEXT_MESSAGE_APP,
    portnums_pb2.PortNum.POSITION_APP,
    portnums_pb2.PortNum.TELEMETRY_APP,
    portnums_pb2.PortNum.MAP_REPORT_APP,
}
if hasattr(portnums_pb2.PortNum, "TEXT_MESSAGE_COMPRESSED_APP"):
    PUBLIC_UPLINK_PORTNUMS.add(portnums_pb2.PortNum.TEXT_MESSAGE_COMPRESSED_APP)

# Downlink safety list — constraints what traffic from MQTT is allowed onto the RF link
PUBLIC_DOWNLINK_PORTNUMS = {
    portnums_pb2.PortNum.TEXT_MESSAGE_APP,
    portnums_pb2.PortNum.POSITION_APP,
}
if hasattr(portnums_pb2.PortNum, "TEXT_MESSAGE_COMPRESSED_APP"):
    PUBLIC_DOWNLINK_PORTNUMS.add(portnums_pb2.PortNum.TEXT_MESSAGE_COMPRESSED_APP)


# =====================================================================
#  INSPECTION AND ROUTING LOGIC ENGINE
# =====================================================================

def inspect_and_filter_packet(raw_bytes: bytes, source_port: int, telemetry_interval_sec: int = 1800) -> dict:
    """
    Parses and enforces routing/timing policies on binary Meshtastic packet streams.
    Returns a dictionary detailing structural validity and target port routing availability.
    """
    global _last_telemetry_mqtt_time
    
    # Default execution routing matrix state
    result = {
        "valid": False,
        "to_client": True,   # Eligible for local UI visualization (Port 4403)
        "to_mqtt": False,    # Eligible for MQTT Proxy Uplink (Port 4404)
        "packet_id": None,
        "hop_limit": None
    }

    # -----------------------------------------------------------------
    #  CASE 1: INCOMING PACKET FROM HARDWARE LAYER (SOURCE_PORT == 0)
    # -----------------------------------------------------------------
    if source_port == 0:
        try:
            from_radio = mesh_pb2.FromRadio()
            from_radio.ParseFromString(raw_bytes)
            
            # Intercept standard mesh packets containing payloads
            if from_radio.HasField("packet"):
                mp = from_radio.packet
                result["valid"] = True
                result["packet_id"] = mp.id
                result["hop_limit"] = mp.hop_limit
                
                # Check for structural hops integrity bounds
                if mp.hop_limit <= 0:
                    logger.debug(f"[FILTER-UPLINK] Packet {mp.id} dropped for MQTT uplink: Hop limit exhausted ({mp.hop_limit}).")
                    result["to_mqtt"] = False
                    return result
                
                # Inspect internal payload properties if decompressed/decoded
                if mp.HasField("decoded"):
                    portnum = mp.decoded.portnum
                    
                    # Validate against priority port numbers allowlist
                    if portnum in PUBLIC_UPLINK_PORTNUMS:
                        result["to_mqtt"] = True
                        
                        # Apply local rate-limiting window exclusively to high-cadence telemetry packets
                        if portnum == portnums_pb2.PortNum.TELEMETRY_APP:
                            current_time = time.time()
                            elapsed = current_time - _last_telemetry_mqtt_time
                            
                            if elapsed < telemetry_interval_sec:
                                logger.debug(f"[FILTER-RATE] Telemetry packet {mp.id} muted for MQTT. Cooldown: {int(elapsed)}/{telemetry_interval_sec}s.")
                                result["to_mqtt"] = False
                            else:
                                _last_telemetry_mqtt_time = current_time
                                logger.info(f"[FILTER-RATE] Telemetry packet {mp.id} allowed for uplink. Window reset.")
                    else:
                        logger.debug(f"[FILTER-UPLINK] Packet {mp.id} hidden from MQTT: PortNum {portnum} not in public profile.")
                
                return result
                
            # Allow control/handshake radio packets straight to local UI exclusively
            elif from_radio.HasField("config_complete") or from_radio.HasField("rebooted") or from_radio.HasField("node_info"):
                result["valid"] = True
                result["to_mqtt"] = False
                return result

        except Exception as e:
            logger.debug(f"[PARSER-ERR] FromRadio structural translation bypassed (Service/Raw packet): {e}")

    # -----------------------------------------------------------------
    #  CASE 2: OUTBOUND PACKET COMING FROM PORT LISTENERS (4403 / 4404)
    # -----------------------------------------------------------------
    elif source_port in [4403, 4404]:
        try:
            to_radio = mesh_pb2.ToRadio()
            to_radio.ParseFromString(raw_bytes)
            
            if to_radio.HasField("packet"):
                mp = to_radio.packet
                result["valid"] = True
                result["packet_id"] = mp.id
                result["hop_limit"] = mp.hop_limit
                
                if mp.HasField("decoded"):
                    portnum = mp.decoded.portnum
                    
                    # Strict validation rules for data ingested from the MQTT proxy interface
                    if source_port == 4404:
                        result["to_client"] = False  # Prevent proxy echo loops to local UI
                        
                        if portnum in PUBLIC_DOWNLINK_PORTNUMS:
                            result["to_mqtt"] = False  # Downlink routing validation confirmed
                        else:
                            # Block malicious, noisy or recursive proxy loops from hitting the RF frontend
                            logger.warning(f"[FILTER-DOWNLINK] Blocked packet from MQTT to RF: PortNum {portnums_pb2.PortNum.Name(portnum)} not allowed.")
                            result["valid"] = False  
                            
                return result
        except Exception as e:
            logger.debug(f"[PARSER-ERR] ToRadio structural translation bypassed on port {source_port} (Service/Raw packet): {e}")
            
    # Safe fallback validation for non-protobuf/raw administrative payloads originating from client software
    if source_port in [4403, 4404]:
        result["valid"] = True
    
    return result

