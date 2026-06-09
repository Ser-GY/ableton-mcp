# ableton_mcp_server.py
from mcp.server.fastmcp import FastMCP, Context
import socket
import json
import logging
import os
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Any, List, Union

ABLETON_HOST = os.environ.get("ABLETON_HOST", "localhost")
ABLETON_PORT = int(os.environ.get("ABLETON_PORT", "9877"))

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AbletonMCPServer")

@dataclass
class AbletonConnection:
    host: str
    port: int
    sock: socket.socket = None

    def connect(self) -> bool:
        """Connect to the Ableton Remote Script socket server"""
        if self.sock:
            return True

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(5.0)
            self.sock.connect((self.host, self.port))
            self.sock.settimeout(None)
            logger.info(f"Connected to Ableton at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Ableton at {self.host}:{self.port}: {str(e)}")
            self.sock = None
            return False

    def disconnect(self):
        """Disconnect from the Ableton Remote Script"""
        if self.sock:
            try:
                self.sock.close()
            except Exception as e:
                logger.error(f"Error disconnecting from Ableton: {str(e)}")
            finally:
                self.sock = None

    def receive_full_response(self, sock, buffer_size=8192):
        """Receive the complete response, potentially in multiple chunks"""
        chunks = []
        sock.settimeout(15.0)  # Increased timeout for operations that might take longer

        try:
            while True:
                try:
                    chunk = sock.recv(buffer_size)
                    if not chunk:
                        if not chunks:
                            raise Exception("Connection closed before receiving any data")
                        break

                    chunks.append(chunk)

                    # Check if we've received a complete JSON object
                    try:
                        data = b''.join(chunks)
                        json.loads(data.decode('utf-8'))
                        logger.info(f"Received complete response ({len(data)} bytes)")
                        return data
                    except json.JSONDecodeError:
                        # Incomplete JSON, continue receiving
                        continue
                except socket.timeout:
                    logger.warning("Socket timeout during chunked receive")
                    break
                except (ConnectionError, BrokenPipeError, ConnectionResetError) as e:
                    logger.error(f"Socket connection error during receive: {str(e)}")
                    raise
        except Exception as e:
            logger.error(f"Error during receive: {str(e)}")
            raise

        # If we get here, we either timed out or broke out of the loop
        if chunks:
            data = b''.join(chunks)
            logger.info(f"Returning data after receive completion ({len(data)} bytes)")
            try:
                json.loads(data.decode('utf-8'))
                return data
            except json.JSONDecodeError:
                raise Exception("Incomplete JSON response received")
        else:
            raise Exception("No data received")

    def send_command(self, command_type: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Send a command to Ableton and return the response"""
        if not self.sock and not self.connect():
            raise ConnectionError("Not connected to Ableton")

        command = {
            "type": command_type,
            "params": params or {}
        }

        # Check if this is a state-modifying command
        is_modifying_command = command_type in [
            "create_midi_track", "create_audio_track", "set_track_name",
            "create_clip", "create_audio_clip", "add_notes_to_clip", "set_clip_name",
            "set_tempo", "fire_clip", "stop_clip", "set_device_parameter",
            "start_playback", "stop_playback", "load_instrument_or_effect",
            # Arrangement view commands
            "switch_to_arrangement_view", "set_current_song_time",
            "duplicate_session_clip_to_arrangement"
        ]

        # Commands whose work on Live's main thread can take noticeably longer
        # than the default modifying-command budget (e.g. importing/decoding a
        # large audio file). Give them a wider socket timeout so we don't time
        # out before the Remote Script's own queue does.
        long_running_commands = {"create_audio_clip": 65.0}

        try:
            logger.info(f"Sending command: {command_type} with params: {params}")

            # Send the command
            self.sock.sendall(json.dumps(command).encode('utf-8'))
            logger.info(f"Command sent, waiting for response...")

            # Set timeout based on command type
            if command_type in long_running_commands:
                timeout = long_running_commands[command_type]
            else:
                timeout = 15.0 if is_modifying_command else 10.0
            self.sock.settimeout(timeout)

            # Receive the response
            response_data = self.receive_full_response(self.sock)
            logger.info(f"Received {len(response_data)} bytes of data")

            # Parse the response
            response = json.loads(response_data.decode('utf-8'))
            logger.info(f"Response parsed, status: {response.get('status', 'unknown')}")

            if response.get("status") == "error":
                logger.error(f"Ableton error: {response.get('message')}")
                raise Exception(response.get("message", "Unknown error from Ableton"))

            return response.get("result", {})
        except socket.timeout:
            logger.error("Socket timeout while waiting for response from Ableton")
            self.sock = None
            raise Exception("Timeout waiting for Ableton response")
        except (ConnectionError, BrokenPipeError, ConnectionResetError) as e:
            logger.error(f"Socket connection error: {str(e)}")
            self.sock = None
            raise Exception(f"Connection to Ableton lost: {str(e)}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from Ableton: {str(e)}")
            if 'response_data' in locals() and response_data:
                logger.error(f"Raw response (first 200 bytes): {response_data[:200]}")
            self.sock = None
            raise Exception(f"Invalid response from Ableton: {str(e)}")
        except Exception as e:
            logger.error(f"Error communicating with Ableton: {str(e)}")
            self.sock = None
            raise Exception(f"Communication error with Ableton: {str(e)}")

@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[Dict[str, Any]]:
    """Manage server startup and shutdown lifecycle"""
    try:
        logger.info("AbletonMCP server starting up")

        try:
            ableton = get_ableton_connection()
            logger.info("Successfully connected to Ableton on startup")
        except Exception as e:
            logger.warning(f"Could not connect to Ableton on startup: {str(e)}")
            logger.warning("Make sure the Ableton Remote Script is running")

        yield {}
    finally:
        global _ableton_connection
        if _ableton_connection:
            logger.info("Disconnecting from Ableton on shutdown")
            _ableton_connection.disconnect()
            _ableton_connection = None
        logger.info("AbletonMCP server shut down")

# Create the MCP server with lifespan support
mcp = FastMCP(
    "AbletonMCP",
    lifespan=server_lifespan
)

# Global connection for resources
_ableton_connection = None

def get_ableton_connection():
    """Get or create a persistent Ableton connection"""
    global _ableton_connection

    if _ableton_connection is not None and _ableton_connection.sock is not None:
        try:
            # Check if the socket is still alive by peeking for data
            # MSG_PEEK + MSG_DONTWAIT will raise BlockingIOError if alive but no data,
            # or return b'' if the remote end has closed the connection.
            _ableton_connection.sock.setblocking(False)
            try:
                data = _ableton_connection.sock.recv(1, socket.MSG_PEEK)
                if data == b'':
                    raise ConnectionError("Remote end closed")
            except BlockingIOError:
                pass  # Socket is alive, just no data waiting — this is normal
            finally:
                _ableton_connection.sock.setblocking(True)
            return _ableton_connection
        except Exception as e:
            logger.warning(f"Existing connection is no longer valid: {str(e)}")
            try:
                _ableton_connection.disconnect()
            except:
                pass
            _ableton_connection = None

    # Connection doesn't exist or is invalid, create a new one
    if _ableton_connection is None:
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Connecting to Ableton at {ABLETON_HOST}:{ABLETON_PORT} (attempt {attempt}/{max_attempts})...")
                _ableton_connection = AbletonConnection(host=ABLETON_HOST, port=ABLETON_PORT)
                if _ableton_connection.connect():
                    logger.info("Created new persistent connection to Ableton")
                    return _ableton_connection
                else:
                    _ableton_connection = None
            except Exception as e:
                logger.error(f"Connection attempt {attempt} failed: {str(e)}")
                if _ableton_connection:
                    _ableton_connection.disconnect()
                    _ableton_connection = None

            if attempt < max_attempts:
                import time
                time.sleep(1.0)

        # If we get here, all connection attempts failed
        if _ableton_connection is None:
            logger.error("Failed to connect to Ableton after multiple attempts")
            raise Exception("Could not connect to Ableton. Make sure the Remote Script is running.")

    return _ableton_connection


# Core Tool endpoints

@mcp.tool()
def get_session_info(ctx: Context) -> str:
    """Get detailed information about the current Ableton session"""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_session_info")
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting session info from Ableton: {str(e)}")
        return f"Error getting session info: {str(e)}"

@mcp.tool()
def get_track_info(ctx: Context, track_index: int) -> str:
    """
    Get detailed information about a specific track in Ableton.

    Parameters:
    - track_index: The index of the track to get information about
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_track_info", {"track_index": track_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting track info from Ableton: {str(e)}")
        return f"Error getting track info: {str(e)}"

@mcp.tool()
def create_midi_track(ctx: Context, index: int = -1) -> str:
    """
    Create a new MIDI track in the Ableton session.

    Parameters:
    - index: The index to insert the track at (-1 = end of list)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_midi_track", {"index": index})
        return f"Created new MIDI track: {result.get('name', 'unknown')}"
    except Exception as e:
        logger.error(f"Error creating MIDI track: {str(e)}")
        return f"Error creating MIDI track: {str(e)}"


@mcp.tool()
def set_track_name(ctx: Context, track_index: int, name: str) -> str:
    """
    Set the name of a track.

    Parameters:
    - track_index: The index of the track to rename
    - name: The new name for the track
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_name", {"track_index": track_index, "name": name})
        return f"Renamed track to: {result.get('name', name)}"
    except Exception as e:
        logger.error(f"Error setting track name: {str(e)}")
        return f"Error setting track name: {str(e)}"

@mcp.tool()
def create_clip(ctx: Context, track_index: int, clip_index: int, length: float = 4.0) -> str:
    """
    Create a new MIDI clip in the specified track and clip slot.

    Parameters:
    - track_index: The index of the track to create the clip in
    - clip_index: The index of the clip slot to create the clip in
    - length: The length of the clip in beats (default: 4.0)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_clip", {
            "track_index": track_index,
            "clip_index": clip_index,
            "length": length
        })
        return f"Created new clip at track {track_index}, slot {clip_index} with length {length} beats"
    except Exception as e:
        logger.error(f"Error creating clip: {str(e)}")
        return f"Error creating clip: {str(e)}"

@mcp.tool()
def create_audio_clip(ctx: Context, track_index: int, clip_index: int, path: str) -> str:
    """
    Create a new audio clip in an audio track's clip slot by importing a file.

    Requires Ableton Live 12.0.5 or newer — the underlying
    ClipSlot.create_audio_clip Live API was introduced in 12.0.5 and is not
    available in earlier 12.0.x releases.

    Parameters:
    - track_index: The index of the audio track to create the clip in
    - clip_index: The index of the clip slot to create the clip in
    - path: Absolute path to a supported audio file (e.g. a .wav). The target
      track must be an audio track and the clip slot must be empty.
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_audio_clip", {
            "track_index": track_index,
            "clip_index": clip_index,
            "path": path
        })
        return f"Created audio clip '{result.get('name', 'clip')}' at track {track_index}, slot {clip_index} (length {result.get('length', '?')} beats)"
    except Exception as e:
        logger.error(f"Error creating audio clip: {str(e)}")
        return f"Error creating audio clip: {str(e)}"

@mcp.tool()
def add_notes_to_clip(
    ctx: Context,
    track_index: int,
    clip_index: int,
    notes: List[Dict[str, Union[int, float, bool]]],
) -> str:
    """
    Add MIDI notes to a clip.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    - notes: List of note dictionaries, each with pitch, start_time, duration, velocity, and mute
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("add_notes_to_clip", {
            "track_index": track_index,
            "clip_index": clip_index,
            "notes": notes
        })
        return f"Added {len(notes)} notes to clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        logger.error(f"Error adding notes to clip: {str(e)}")
        return f"Error adding notes to clip: {str(e)}"

@mcp.tool()
def set_clip_name(ctx: Context, track_index: int, clip_index: int, name: str) -> str:
    """
    Set the name of a clip.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    - name: The new name for the clip
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_clip_name", {
            "track_index": track_index,
            "clip_index": clip_index,
            "name": name
        })
        return f"Renamed clip at track {track_index}, slot {clip_index} to '{name}'"
    except Exception as e:
        logger.error(f"Error setting clip name: {str(e)}")
        return f"Error setting clip name: {str(e)}"

@mcp.tool()
def set_tempo(ctx: Context, tempo: float) -> str:
    """
    Set the tempo of the Ableton session.

    Parameters:
    - tempo: The new tempo in BPM
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_tempo", {"tempo": tempo})
        return f"Set tempo to {tempo} BPM"
    except Exception as e:
        logger.error(f"Error setting tempo: {str(e)}")
        return f"Error setting tempo: {str(e)}"


@mcp.tool()
def load_instrument_or_effect(ctx: Context, track_index: int, uri: str) -> str:
    """
    Load an instrument or effect onto a track using its URI.

    Parameters:
    - track_index: The index of the track to load the instrument on
    - uri: The URI of the instrument or effect to load (e.g., 'query:Synths#Instrument%20Rack:Bass:FileId_5116')
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("load_browser_item", {
            "track_index": track_index,
            "item_uri": uri
        })

        # Check if the instrument was loaded successfully
        if result.get("loaded", False):
            new_devices = result.get("new_devices", [])
            if new_devices:
                return f"Loaded instrument with URI '{uri}' on track {track_index}. New devices: {', '.join(new_devices)}"
            else:
                devices = result.get("devices_after", [])
                return f"Loaded instrument with URI '{uri}' on track {track_index}. Devices on track: {', '.join(devices)}"
        else:
            return f"Failed to load instrument with URI '{uri}'"
    except Exception as e:
        logger.error(f"Error loading instrument by URI: {str(e)}")
        return f"Error loading instrument by URI: {str(e)}"

@mcp.tool()
def fire_clip(ctx: Context, track_index: int, clip_index: int) -> str:
    """
    Start playing a clip.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("fire_clip", {
            "track_index": track_index,
            "clip_index": clip_index
        })
        return f"Started playing clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        logger.error(f"Error firing clip: {str(e)}")
        return f"Error firing clip: {str(e)}"

@mcp.tool()
def stop_clip(ctx: Context, track_index: int, clip_index: int) -> str:
    """
    Stop playing a clip.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("stop_clip", {
            "track_index": track_index,
            "clip_index": clip_index
        })
        return f"Stopped clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        logger.error(f"Error stopping clip: {str(e)}")
        return f"Error stopping clip: {str(e)}"

@mcp.tool()
def start_playback(ctx: Context) -> str:
    """Start playing the Ableton session."""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("start_playback")
        return "Started playback"
    except Exception as e:
        logger.error(f"Error starting playback: {str(e)}")
        return f"Error starting playback: {str(e)}"

@mcp.tool()
def stop_playback(ctx: Context) -> str:
    """Stop playing the Ableton session."""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("stop_playback")
        return "Stopped playback"
    except Exception as e:
        logger.error(f"Error stopping playback: {str(e)}")
        return f"Error stopping playback: {str(e)}"

@mcp.tool()
def get_browser_tree(ctx: Context, category_type: str = "all") -> str:
    """
    Get a hierarchical tree of browser categories from Ableton.

    Parameters:
    - category_type: Type of categories to get ('all', 'instruments', 'sounds', 'drums', 'audio_effects', 'midi_effects')
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_browser_tree", {
            "category_type": category_type
        })

        # Check if we got any categories
        if "available_categories" in result and len(result.get("categories", [])) == 0:
            available_cats = result.get("available_categories", [])
            return (f"No categories found for '{category_type}'. "
                   f"Available browser categories: {', '.join(available_cats)}")

        # Format the tree in a more readable way
        total_folders = result.get("total_folders", 0)
        formatted_output = f"Browser tree for '{category_type}' (showing {total_folders} folders):\n\n"

        def format_tree(item, indent=0):
            output = ""
            if item:
                prefix = "  " * indent
                name = item.get("name", "Unknown")
                path = item.get("path", "")
                has_more = item.get("has_more", False)

                # Add this item
                output += f"{prefix}• {name}"
                if path:
                    output += f" (path: {path})"
                if has_more:
                    output += " [...]"
                output += "\n"

                # Add children
                for child in item.get("children", []):
                    output += format_tree(child, indent + 1)
            return output

        # Format each category
        for category in result.get("categories", []):
            formatted_output += format_tree(category)
            formatted_output += "\n"

        return formatted_output
    except Exception as e:
        error_msg = str(e)
        if "Browser is not available" in error_msg:
            logger.error(f"Browser is not available in Ableton: {error_msg}")
            return f"Error: The Ableton browser is not available. Make sure Ableton Live is fully loaded and try again."
        elif "Could not access Live application" in error_msg:
            logger.error(f"Could not access Live application: {error_msg}")
            return f"Error: Could not access the Ableton Live application. Make sure Ableton Live is running and the Remote Script is loaded."
        else:
            logger.error(f"Error getting browser tree: {error_msg}")
            return f"Error getting browser tree: {error_msg}"

@mcp.tool()
def get_browser_items_at_path(ctx: Context, path: str) -> str:
    """
    Get browser items at a specific path in Ableton's browser.

    Parameters:
    - path: Path in the format "category/folder/subfolder"
            where category is one of the available browser categories in Ableton
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_browser_items_at_path", {
            "path": path
        })

        # Check if there was an error with available categories
        if "error" in result and "available_categories" in result:
            error = result.get("error", "")
            available_cats = result.get("available_categories", [])
            return (f"Error: {error}\n"
                   f"Available browser categories: {', '.join(available_cats)}")

        return json.dumps(result, indent=2)
    except Exception as e:
        error_msg = str(e)
        if "Browser is not available" in error_msg:
            logger.error(f"Browser is not available in Ableton: {error_msg}")
            return f"Error: The Ableton browser is not available. Make sure Ableton Live is fully loaded and try again."
        elif "Could not access Live application" in error_msg:
            logger.error(f"Could not access Live application: {error_msg}")
            return f"Error: Could not access the Ableton Live application. Make sure Ableton Live is running and the Remote Script is loaded."
        elif "Unknown or unavailable category" in error_msg:
            logger.error(f"Invalid browser category: {error_msg}")
            return f"Error: {error_msg}. Please check the available categories using get_browser_tree."
        elif "Path part" in error_msg and "not found" in error_msg:
            logger.error(f"Path not found: {error_msg}")
            return f"Error: {error_msg}. Please check the path and try again."
        else:
            logger.error(f"Error getting browser items at path: {error_msg}")
            return f"Error getting browser items at path: {error_msg}"

@mcp.tool()
def load_drum_kit(ctx: Context, track_index: int, rack_uri: str, kit_path: str) -> str:
    """
    Load a drum rack and then load a specific drum kit into it.

    Parameters:
    - track_index: The index of the track to load on
    - rack_uri: The URI of the drum rack to load (e.g., 'Drums/Drum Rack')
    - kit_path: Path to the drum kit inside the browser (e.g., 'drums/acoustic/kit1')
    """
    try:
        ableton = get_ableton_connection()

        # Step 1: Load the drum rack
        result = ableton.send_command("load_browser_item", {
            "track_index": track_index,
            "item_uri": rack_uri
        })

        if not result.get("loaded", False):
            return f"Failed to load drum rack with URI '{rack_uri}'"

        # Step 2: Get the drum kit items at the specified path
        kit_result = ableton.send_command("get_browser_items_at_path", {
            "path": kit_path
        })

        if "error" in kit_result:
            return f"Loaded drum rack but failed to find drum kit: {kit_result.get('error')}"

        # Step 3: Find a loadable drum kit
        kit_items = kit_result.get("items", [])
        loadable_kits = [item for item in kit_items if item.get("is_loadable", False)]

        if not loadable_kits:
            return f"Loaded drum rack but no loadable drum kits found at '{kit_path}'"

        # Step 4: Load the first loadable kit
        kit_uri = loadable_kits[0].get("uri")
        load_result = ableton.send_command("load_browser_item", {
            "track_index": track_index,
            "item_uri": kit_uri
        })

        return f"Loaded drum rack and kit '{loadable_kits[0].get('name')}' on track {track_index}"
    except Exception as e:
        logger.error(f"Error loading drum kit: {str(e)}")
        return f"Error loading drum kit: {str(e)}"

# ── Arrangement view tools ────────────────────────────────────────────────────

@mcp.tool()
def switch_to_arrangement_view(ctx: Context) -> str:
    """Switch Ableton's main window to the Arrangement view."""
    try:
        ableton = get_ableton_connection()
        ableton.send_command("switch_to_arrangement_view")
        return "Switched to Arrangement view"
    except Exception as e:
        logger.error(f"Error switching to arrangement view: {str(e)}")
        return f"Error switching to arrangement view: {str(e)}"


@mcp.tool()
def set_arrangement_time(ctx: Context, time: float) -> str:
    """
    Move the arrangement playhead to a specific position.

    Parameters:
    - time: Position in beats from the start of the arrangement (e.g. 8.0 = bar 3 in 4/4)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_current_song_time", {"time": time})
        return f"Playhead moved to beat {result.get('current_song_time', time)}"
    except Exception as e:
        logger.error(f"Error setting arrangement time: {str(e)}")
        return f"Error setting arrangement time: {str(e)}"


@mcp.tool()
def get_arrangement_clips(ctx: Context, track_index: int) -> str:
    """
    List all clips placed in the Arrangement timeline for a track.

    Returns each clip's name, start_time, end_time, length, and type.

    Parameters:
    - track_index: The index of the track to inspect
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_arrangement_clips", {"track_index": track_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting arrangement clips: {str(e)}")
        return f"Error getting arrangement clips: {str(e)}"


@mcp.tool()
def duplicate_to_arrangement(
    ctx: Context,
    track_index: int,
    clip_index: int,
    destination_time: float,
) -> str:
    """
    Copy a Session-view clip into the Arrangement timeline.

    Uses Live's track.duplicate_clip_to_arrangement() API (Live 11 / 12).
    The clip is placed at destination_time beats from the start of the
    arrangement on the same track it lives in.

    Typical workflow:
      1. create_clip / add_notes_to_clip to build a Session clip
      2. Call duplicate_to_arrangement once per bar/section you need
      3. Call switch_to_arrangement_view to confirm the result in Live

    Parameters:
    - track_index:       Index of the track that owns the Session clip
    - clip_index:        Index of the clip slot in that track (Session view)
    - destination_time:  Beat position in the arrangement to place the clip
                         (e.g. 0.0 = start, 8.0 = bar 3 in 4/4)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command(
            "duplicate_session_clip_to_arrangement",
            {
                "track_index": track_index,
                "clip_index": clip_index,
                "destination_time": destination_time
            }
        )
        clip_name = result.get("clip_name", "clip")
        track_name = result.get("track_name", f"track {track_index}")
        return (
            f"Duplicated '{clip_name}' from Session slot {clip_index} "
            f"on '{track_name}' to arrangement at beat {destination_time}"
        )
    except Exception as e:
        logger.error(f"Error duplicating clip to arrangement: {str(e)}")
        return f"Error duplicating clip to arrangement: {str(e)}"


# ── Track management ─────────────────────────────────────────────────────────

@mcp.tool()
def create_audio_track(ctx: Context, index: int = -1) -> str:
    """
    Create a new audio track in the Ableton session.

    Parameters:
    - index: The index to insert the track at (-1 = end of list)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_audio_track", {"index": index})
        return f"Created new audio track '{result.get('name', 'unknown')}' at index {result.get('index')}"
    except Exception as e:
        return f"Error creating audio track: {str(e)}"


@mcp.tool()
def delete_track(ctx: Context, track_index: int) -> str:
    """
    Delete a track from the session.

    Parameters:
    - track_index: The index of the track to delete
    """
    try:
        ableton = get_ableton_connection()
        ableton.send_command("delete_track", {"track_index": track_index})
        return f"Deleted track at index {track_index}"
    except Exception as e:
        return f"Error deleting track: {str(e)}"


@mcp.tool()
def duplicate_track(ctx: Context, track_index: int) -> str:
    """
    Duplicate a track. The copy is inserted immediately after the original.

    Parameters:
    - track_index: The index of the track to duplicate
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("duplicate_track", {"track_index": track_index})
        return f"Duplicated track {track_index} — copy at index {result.get('new_index')}"
    except Exception as e:
        return f"Error duplicating track: {str(e)}"


@mcp.tool()
def create_return_track(ctx: Context) -> str:
    """Create a new return (send) track in the session."""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_return_track")
        return f"Created return track '{result.get('name', 'unknown')}' at index {result.get('index')}"
    except Exception as e:
        return f"Error creating return track: {str(e)}"


# ── Track mixer controls ──────────────────────────────────────────────────────

@mcp.tool()
def set_track_volume(ctx: Context, track_index: int, volume: float) -> str:
    """
    Set the volume of a track. Range 0.0–1.0; 0.85 ≈ 0 dB.

    Parameters:
    - track_index: Index of the track
    - volume: Volume level (0.0 = silent, 0.85 = 0 dB, 1.0 = +6 dB)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_volume", {"track_index": track_index, "volume": volume})
        return f"Track {track_index} volume set to {result.get('volume'):.3f}"
    except Exception as e:
        return f"Error setting track volume: {str(e)}"


@mcp.tool()
def set_track_pan(ctx: Context, track_index: int, pan: float) -> str:
    """
    Set the panning of a track. Range -1.0 (full left) to 1.0 (full right); 0.0 = center.

    Parameters:
    - track_index: Index of the track
    - pan: Pan value (-1.0 to 1.0)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_pan", {"track_index": track_index, "pan": pan})
        return f"Track {track_index} pan set to {result.get('panning'):.3f}"
    except Exception as e:
        return f"Error setting track pan: {str(e)}"


@mcp.tool()
def set_track_mute(ctx: Context, track_index: int, mute: bool) -> str:
    """
    Mute or unmute a track.

    Parameters:
    - track_index: Index of the track
    - mute: True to mute, False to unmute
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_mute", {"track_index": track_index, "mute": mute})
        state = "muted" if result.get("mute") else "unmuted"
        return f"Track {track_index} {state}"
    except Exception as e:
        return f"Error setting track mute: {str(e)}"


@mcp.tool()
def set_track_solo(ctx: Context, track_index: int, solo: bool) -> str:
    """
    Solo or unsolo a track.

    Parameters:
    - track_index: Index of the track
    - solo: True to solo, False to unsolo
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_solo", {"track_index": track_index, "solo": solo})
        state = "soloed" if result.get("solo") else "unsoloed"
        return f"Track {track_index} {state}"
    except Exception as e:
        return f"Error setting track solo: {str(e)}"


@mcp.tool()
def set_track_arm(ctx: Context, track_index: int, arm: bool) -> str:
    """
    Arm or disarm a track for recording.

    Parameters:
    - track_index: Index of the track
    - arm: True to arm, False to disarm
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_arm", {"track_index": track_index, "arm": arm})
        state = "armed" if result.get("arm") else "disarmed"
        return f"Track {track_index} {state}"
    except Exception as e:
        return f"Error setting track arm: {str(e)}"


@mcp.tool()
def set_track_color(ctx: Context, track_index: int, color: int) -> str:
    """
    Set the color of a track using an RGB integer.

    Parameters:
    - track_index: Index of the track
    - color: RGB color as integer (e.g. 16711680 = 0xFF0000 = red)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_color", {"track_index": track_index, "color": color})
        return f"Track {track_index} color set to {result.get('color')}"
    except Exception as e:
        return f"Error setting track color: {str(e)}"


# ── Device control ────────────────────────────────────────────────────────────

@mcp.tool()
def get_device_parameters(ctx: Context, track_index: int, device_index: int) -> str:
    """
    List all parameters for a device on a track, including current values, ranges, and discrete options.

    Parameters:
    - track_index: Index of the track
    - device_index: Index of the device on that track
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_device_parameters", {"track_index": track_index, "device_index": device_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting device parameters: {str(e)}"


@mcp.tool()
def set_device_parameter(ctx: Context, track_index: int, device_index: int, parameter_index: int, value: float) -> str:
    """
    Set a parameter value on a device. Use get_device_parameters first to find the right index and value range.

    Parameters:
    - track_index: Index of the track
    - device_index: Index of the device on that track
    - parameter_index: Index of the parameter within the device
    - value: New value (must be within the parameter's min/max range)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_device_parameter", {
            "track_index": track_index, "device_index": device_index,
            "parameter_index": parameter_index, "value": value
        })
        return f"Set '{result.get('parameter')}' to {result.get('value'):.4f}"
    except Exception as e:
        return f"Error setting device parameter: {str(e)}"


# ── Clip operations ───────────────────────────────────────────────────────────

@mcp.tool()
def delete_clip(ctx: Context, track_index: int, clip_index: int) -> str:
    """
    Delete a clip from a clip slot.

    Parameters:
    - track_index: Index of the track
    - clip_index: Index of the clip slot
    """
    try:
        ableton = get_ableton_connection()
        ableton.send_command("delete_clip", {"track_index": track_index, "clip_index": clip_index})
        return f"Deleted clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        return f"Error deleting clip: {str(e)}"


@mcp.tool()
def get_clip_notes(ctx: Context, track_index: int, clip_index: int) -> str:
    """
    Get all MIDI notes in a clip.

    Parameters:
    - track_index: Index of the track
    - clip_index: Index of the clip slot
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_clip_notes", {"track_index": track_index, "clip_index": clip_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting clip notes: {str(e)}"


@mcp.tool()
def delete_notes_from_clip(
    ctx: Context,
    track_index: int,
    clip_index: int,
    from_time: float = 0.0,
    time_span: float = 1.0,
    from_pitch: int = 0,
    pitch_span: int = 128,
) -> str:
    """
    Delete MIDI notes from a clip within a time and pitch range.

    Parameters:
    - track_index: Index of the track
    - clip_index: Index of the clip slot
    - from_time: Start time in beats
    - time_span: Duration of the range in beats
    - from_pitch: Lowest MIDI pitch to remove (0–127)
    - pitch_span: Number of pitches to cover (default 128 = all)
    """
    try:
        ableton = get_ableton_connection()
        ableton.send_command("delete_notes_from_clip", {
            "track_index": track_index, "clip_index": clip_index,
            "from_time": from_time, "time_span": time_span,
            "from_pitch": from_pitch, "pitch_span": pitch_span
        })
        return f"Deleted notes from track {track_index}, slot {clip_index} (time {from_time}–{from_time+time_span}, pitch {from_pitch}–{from_pitch+pitch_span-1})"
    except Exception as e:
        return f"Error deleting notes from clip: {str(e)}"


@mcp.tool()
def set_clip_loop(
    ctx: Context,
    track_index: int,
    clip_index: int,
    looping: bool,
    loop_start: float = None,
    loop_end: float = None,
) -> str:
    """
    Set loop settings on a clip.

    Parameters:
    - track_index: Index of the track
    - clip_index: Index of the clip slot
    - looping: Enable or disable looping
    - loop_start: Loop start position in beats (optional)
    - loop_end: Loop end position in beats (optional)
    """
    try:
        ableton = get_ableton_connection()
        params = {"track_index": track_index, "clip_index": clip_index, "looping": looping}
        if loop_start is not None:
            params["loop_start"] = loop_start
        if loop_end is not None:
            params["loop_end"] = loop_end
        result = ableton.send_command("set_clip_loop", params)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error setting clip loop: {str(e)}"


@mcp.tool()
def set_clip_pitch(
    ctx: Context,
    track_index: int,
    clip_index: int,
    coarse: int = None,
    fine: float = None,
) -> str:
    """
    Adjust pitch of an audio clip (audio clips only).

    Parameters:
    - track_index: Index of the track
    - clip_index: Index of the clip slot
    - coarse: Semitone transposition (-48 to +48)
    - fine: Fine tune in cents (-500 to +500)
    """
    try:
        ableton = get_ableton_connection()
        params = {"track_index": track_index, "clip_index": clip_index}
        if coarse is not None:
            params["coarse"] = coarse
        if fine is not None:
            params["fine"] = fine
        result = ableton.send_command("set_clip_pitch", params)
        return f"Clip pitch: {result.get('pitch_coarse')} semitones, {result.get('pitch_fine')} cents"
    except Exception as e:
        return f"Error setting clip pitch: {str(e)}"


@mcp.tool()
def set_clip_gain(ctx: Context, track_index: int, clip_index: int, gain: float) -> str:
    """
    Set the gain of an audio clip (audio clips only). Range 0.0–1.0.

    Parameters:
    - track_index: Index of the track
    - clip_index: Index of the clip slot
    - gain: Gain value (0.0 to 1.0)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_clip_gain", {
            "track_index": track_index, "clip_index": clip_index, "gain": gain
        })
        return f"Clip gain set to {result.get('gain'):.3f}"
    except Exception as e:
        return f"Error setting clip gain: {str(e)}"


@mcp.tool()
def set_clip_warp_mode(
    ctx: Context,
    track_index: int,
    clip_index: int,
    warp_mode: int = None,
    warping: bool = None,
) -> str:
    """
    Set warp mode on an audio clip. Warp modes: 0=Beats, 1=Tones, 2=Texture, 3=Re-Pitch, 4=Complex, 5=Complex Pro.

    Parameters:
    - track_index: Index of the track
    - clip_index: Index of the clip slot
    - warp_mode: Warp algorithm (0–5)
    - warping: Enable or disable warping
    """
    try:
        ableton = get_ableton_connection()
        params = {"track_index": track_index, "clip_index": clip_index}
        if warp_mode is not None:
            params["warp_mode"] = warp_mode
        if warping is not None:
            params["warping"] = warping
        result = ableton.send_command("set_clip_warp_mode", params)
        return f"Clip warp: enabled={result.get('warping')}, mode={result.get('warp_mode')}"
    except Exception as e:
        return f"Error setting clip warp mode: {str(e)}"


@mcp.tool()
def set_clip_signature(
    ctx: Context,
    track_index: int,
    clip_index: int,
    numerator: int,
    denominator: int,
) -> str:
    """
    Set the time signature of an individual clip.

    Parameters:
    - track_index: Index of the track
    - clip_index: Index of the clip slot
    - numerator: Time signature numerator (e.g. 4)
    - denominator: Time signature denominator (e.g. 4)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_clip_signature", {
            "track_index": track_index, "clip_index": clip_index,
            "numerator": numerator, "denominator": denominator
        })
        return f"Clip time signature set to {result.get('signature_numerator')}/{result.get('signature_denominator')}"
    except Exception as e:
        return f"Error setting clip signature: {str(e)}"


@mcp.tool()
def duplicate_clip_in_session(
    ctx: Context,
    track_index: int,
    src_clip_index: int,
    dst_clip_index: int,
) -> str:
    """
    Duplicate a clip to another slot on the same track in Session view.

    Parameters:
    - track_index: Index of the track
    - src_clip_index: Source clip slot index
    - dst_clip_index: Destination clip slot index (must be empty)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("duplicate_clip_in_session", {
            "track_index": track_index,
            "src_clip_index": src_clip_index,
            "dst_clip_index": dst_clip_index
        })
        return f"Duplicated clip from slot {src_clip_index} to slot {dst_clip_index} on track {track_index}"
    except Exception as e:
        return f"Error duplicating clip in session: {str(e)}"


# ── Scene operations ──────────────────────────────────────────────────────────

@mcp.tool()
def fire_scene(ctx: Context, scene_index: int) -> str:
    """
    Launch all clips in a scene simultaneously.

    Parameters:
    - scene_index: Index of the scene to fire
    """
    try:
        ableton = get_ableton_connection()
        ableton.send_command("fire_scene", {"scene_index": scene_index})
        return f"Fired scene {scene_index}"
    except Exception as e:
        return f"Error firing scene: {str(e)}"


@mcp.tool()
def create_scene(ctx: Context, index: int = -1) -> str:
    """
    Create a new scene.

    Parameters:
    - index: Position to insert the scene (-1 = end)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_scene", {"index": index})
        return f"Created scene '{result.get('name')}' at index {result.get('index')}"
    except Exception as e:
        return f"Error creating scene: {str(e)}"


@mcp.tool()
def delete_scene(ctx: Context, scene_index: int) -> str:
    """
    Delete a scene.

    Parameters:
    - scene_index: Index of the scene to delete
    """
    try:
        ableton = get_ableton_connection()
        ableton.send_command("delete_scene", {"scene_index": scene_index})
        return f"Deleted scene {scene_index}"
    except Exception as e:
        return f"Error deleting scene: {str(e)}"


@mcp.tool()
def duplicate_scene(ctx: Context, scene_index: int) -> str:
    """
    Duplicate a scene. The copy is inserted immediately after the original.

    Parameters:
    - scene_index: Index of the scene to duplicate
    """
    try:
        ableton = get_ableton_connection()
        ableton.send_command("duplicate_scene", {"scene_index": scene_index})
        return f"Duplicated scene {scene_index}"
    except Exception as e:
        return f"Error duplicating scene: {str(e)}"


@mcp.tool()
def set_scene_name(ctx: Context, scene_index: int, name: str) -> str:
    """
    Rename a scene.

    Parameters:
    - scene_index: Index of the scene
    - name: New name for the scene
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_scene_name", {"scene_index": scene_index, "name": name})
        return f"Scene {scene_index} renamed to '{result.get('name')}'"
    except Exception as e:
        return f"Error setting scene name: {str(e)}"


@mcp.tool()
def set_scene_tempo(ctx: Context, scene_index: int, tempo: float, enabled: bool = True) -> str:
    """
    Assign a tempo to a scene so it changes when the scene is launched.

    Parameters:
    - scene_index: Index of the scene
    - tempo: Tempo in BPM
    - enabled: Whether the tempo change is active (default True)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_scene_tempo", {
            "scene_index": scene_index, "tempo": tempo, "enabled": enabled
        })
        return f"Scene {scene_index} tempo set to {result.get('tempo')} BPM (enabled={result.get('tempo_enabled')})"
    except Exception as e:
        return f"Error setting scene tempo: {str(e)}"


# ── Transport & recording ─────────────────────────────────────────────────────

@mcp.tool()
def set_time_signature(ctx: Context, numerator: int, denominator: int) -> str:
    """
    Set the global time signature of the session.

    Parameters:
    - numerator: Time signature numerator (e.g. 4)
    - denominator: Time signature denominator (e.g. 4)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_time_signature", {"numerator": numerator, "denominator": denominator})
        return f"Time signature set to {result.get('signature_numerator')}/{result.get('signature_denominator')}"
    except Exception as e:
        return f"Error setting time signature: {str(e)}"


@mcp.tool()
def set_loop_points(
    ctx: Context,
    loop_on: bool,
    loop_start: float = None,
    loop_length: float = None,
) -> str:
    """
    Configure the arrangement loop.

    Parameters:
    - loop_on: Enable or disable the loop
    - loop_start: Loop start in beats (optional)
    - loop_length: Loop length in beats (optional)
    """
    try:
        ableton = get_ableton_connection()
        params = {"loop_on": loop_on}
        if loop_start is not None:
            params["loop_start"] = loop_start
        if loop_length is not None:
            params["loop_length"] = loop_length
        result = ableton.send_command("set_loop_points", params)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error setting loop points: {str(e)}"


@mcp.tool()
def continue_playback(ctx: Context) -> str:
    """Resume playback from the current position (rather than restarting from the beginning)."""
    try:
        ableton = get_ableton_connection()
        ableton.send_command("continue_playback")
        return "Resumed playback"
    except Exception as e:
        return f"Error continuing playback: {str(e)}"


@mcp.tool()
def set_metronome(ctx: Context, enabled: bool) -> str:
    """
    Turn the metronome on or off.

    Parameters:
    - enabled: True to enable, False to disable
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_metronome", {"enabled": enabled})
        state = "on" if result.get("metronome") else "off"
        return f"Metronome {state}"
    except Exception as e:
        return f"Error setting metronome: {str(e)}"


@mcp.tool()
def set_record_mode(ctx: Context, enabled: bool) -> str:
    """
    Enable or disable arrangement record mode.

    Parameters:
    - enabled: True to start recording, False to stop
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_record_mode", {"enabled": enabled})
        state = "on" if result.get("record_mode") else "off"
        return f"Record mode {state}"
    except Exception as e:
        return f"Error setting record mode: {str(e)}"


@mcp.tool()
def set_session_record(ctx: Context, enabled: bool) -> str:
    """
    Enable or disable session record mode (clips record on armed tracks).

    Parameters:
    - enabled: True to arm session recording, False to disarm
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_session_record", {"enabled": enabled})
        state = "on" if result.get("session_record") else "off"
        return f"Session record {state}"
    except Exception as e:
        return f"Error setting session record: {str(e)}"


@mcp.tool()
def capture_midi(ctx: Context) -> str:
    """Capture recently played MIDI notes into a new clip (Live's Capture MIDI feature)."""
    try:
        ableton = get_ableton_connection()
        ableton.send_command("capture_midi")
        return "MIDI captured"
    except Exception as e:
        return f"Error capturing MIDI: {str(e)}"


@mcp.tool()
def undo(ctx: Context) -> str:
    """Undo the last action in Ableton."""
    try:
        ableton = get_ableton_connection()
        ableton.send_command("undo")
        return "Undone"
    except Exception as e:
        return f"Error undoing: {str(e)}"


@mcp.tool()
def redo(ctx: Context) -> str:
    """Redo the last undone action in Ableton."""
    try:
        ableton = get_ableton_connection()
        ableton.send_command("redo")
        return "Redone"
    except Exception as e:
        return f"Error redoing: {str(e)}"


@mcp.tool()
def tap_tempo(ctx: Context) -> str:
    """Tap the tempo — call repeatedly to set BPM by tapping."""
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("tap_tempo")
        return f"Tapped tempo — current BPM: {result.get('tempo'):.2f}"
    except Exception as e:
        return f"Error tapping tempo: {str(e)}"


@mcp.tool()
def jump_to_cue(ctx: Context, direction: str = "next") -> str:
    """
    Jump the playhead to the next or previous cue point (arrangement marker).

    Parameters:
    - direction: 'next' or 'prev'
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("jump_to_cue", {"direction": direction})
        return f"Jumped to {direction} cue — position: {result.get('current_song_time'):.2f} beats"
    except Exception as e:
        return f"Error jumping to cue: {str(e)}"


@mcp.tool()
def stop_all_clips(ctx: Context, quantized: bool = True) -> str:
    """
    Stop all currently playing clips in the session.

    Parameters:
    - quantized: If True, stop is quantized to the clip trigger quantization setting
    """
    try:
        ableton = get_ableton_connection()
        ableton.send_command("stop_all_clips", {"quantized": quantized})
        return "Stopped all clips"
    except Exception as e:
        return f"Error stopping all clips: {str(e)}"


# ── Routing ───────────────────────────────────────────────────────────────────

@mcp.tool()
def set_track_input_routing(ctx: Context, track_index: int, routing: str) -> str:
    """
    Set the input routing of a track (e.g. 'Ext. In', '1/2').
    Use get_track_info to inspect available routing options first.

    Parameters:
    - track_index: Index of the track
    - routing: Input routing string as it appears in Live
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_input_routing", {"track_index": track_index, "routing": routing})
        return f"Track {track_index} input routing set to '{result.get('current_input_routing')}'"
    except Exception as e:
        return f"Error setting track input routing: {str(e)}"


@mcp.tool()
def set_track_output_routing(ctx: Context, track_index: int, routing: str) -> str:
    """
    Set the output routing of a track (e.g. 'Master', 'Sends Only').

    Parameters:
    - track_index: Index of the track
    - routing: Output routing string as it appears in Live
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_output_routing", {"track_index": track_index, "routing": routing})
        return f"Track {track_index} output routing set to '{result.get('current_output_routing')}'"
    except Exception as e:
        return f"Error setting track output routing: {str(e)}"


@mcp.tool()
def set_track_monitoring(ctx: Context, track_index: int, monitoring_state: int) -> str:
    """
    Set the monitoring state of a track. 0 = In (always monitor), 1 = Auto (monitor when armed), 2 = Off.

    Parameters:
    - track_index: Index of the track
    - monitoring_state: 0 (In), 1 (Auto), or 2 (Off)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_monitoring", {"track_index": track_index, "monitoring_state": monitoring_state})
        labels = {0: "In", 1: "Auto", 2: "Off"}
        state_label = labels.get(result.get("current_monitoring_state", 1), str(result.get("current_monitoring_state")))
        return f"Track {track_index} monitoring set to {state_label}"
    except Exception as e:
        return f"Error setting track monitoring: {str(e)}"


@mcp.tool()
def set_send_amount(ctx: Context, track_index: int, send_index: int, value: float) -> str:
    """
    Set the send amount from a track to a return track. Range 0.0–1.0.

    Parameters:
    - track_index: Index of the source track
    - send_index: Index of the send (0 = first return track)
    - value: Send amount (0.0 = off, 0.85 ≈ 0 dB, 1.0 = max)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_send_amount", {
            "track_index": track_index, "send_index": send_index, "value": value
        })
        return f"Track {track_index} send {send_index} set to {result.get('value'):.3f}"
    except Exception as e:
        return f"Error setting send amount: {str(e)}"


# ── Master & return tracks ────────────────────────────────────────────────────

@mcp.tool()
def set_master_volume(ctx: Context, volume: float) -> str:
    """
    Set the master track volume. Range 0.0–1.0; 0.85 ≈ 0 dB.

    Parameters:
    - volume: Volume level (0.0 = silent, 0.85 = 0 dB)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_master_volume", {"volume": volume})
        return f"Master volume set to {result.get('volume'):.3f}"
    except Exception as e:
        return f"Error setting master volume: {str(e)}"


@mcp.tool()
def set_master_pan(ctx: Context, pan: float) -> str:
    """
    Set the master track panning. Range -1.0 to 1.0.

    Parameters:
    - pan: Pan value (-1.0 = left, 0.0 = center, 1.0 = right)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_master_pan", {"pan": pan})
        return f"Master pan set to {result.get('panning'):.3f}"
    except Exception as e:
        return f"Error setting master pan: {str(e)}"


@mcp.tool()
def set_crossfader(ctx: Context, value: float) -> str:
    """
    Set the master crossfader position. Range -1.0 (A) to 1.0 (B); 0.0 = center.

    Parameters:
    - value: Crossfader position (-1.0 to 1.0)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_crossfader", {"value": value})
        return f"Crossfader set to {result.get('crossfader'):.3f}"
    except Exception as e:
        return f"Error setting crossfader: {str(e)}"


@mcp.tool()
def get_return_track_info(ctx: Context, track_index: int) -> str:
    """
    Get information about a return (send) track, including devices and mixer state.

    Parameters:
    - track_index: Index of the return track (0 = first return track, e.g. 'A')
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_return_track_info", {"track_index": track_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting return track info: {str(e)}"


@mcp.tool()
def set_return_track_name(ctx: Context, track_index: int, name: str) -> str:
    """
    Rename a return track.

    Parameters:
    - track_index: Index of the return track
    - name: New name
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_return_track_name", {"track_index": track_index, "name": name})
        return f"Return track {track_index} renamed to '{result.get('name')}'"
    except Exception as e:
        return f"Error setting return track name: {str(e)}"


@mcp.tool()
def set_return_track_volume(ctx: Context, track_index: int, volume: float) -> str:
    """
    Set the volume of a return track. Range 0.0–1.0; 0.85 ≈ 0 dB.

    Parameters:
    - track_index: Index of the return track
    - volume: Volume level
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_return_track_volume", {"track_index": track_index, "volume": volume})
        return f"Return track {track_index} volume set to {result.get('volume'):.3f}"
    except Exception as e:
        return f"Error setting return track volume: {str(e)}"


@mcp.tool()
def set_return_track_pan(ctx: Context, track_index: int, pan: float) -> str:
    """
    Set the panning of a return track. Range -1.0 to 1.0.

    Parameters:
    - track_index: Index of the return track
    - pan: Pan value
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_return_track_pan", {"track_index": track_index, "pan": pan})
        return f"Return track {track_index} pan set to {result.get('panning'):.3f}"
    except Exception as e:
        return f"Error setting return track pan: {str(e)}"


@mcp.tool()
def set_return_track_mute(ctx: Context, track_index: int, mute: bool) -> str:
    """
    Mute or unmute a return track.

    Parameters:
    - track_index: Index of the return track
    - mute: True to mute, False to unmute
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_return_track_mute", {"track_index": track_index, "mute": mute})
        state = "muted" if result.get("mute") else "unmuted"
        return f"Return track {track_index} {state}"
    except Exception as e:
        return f"Error setting return track mute: {str(e)}"


# ── Advanced ──────────────────────────────────────────────────────────────────

@mcp.tool()
def get_rack_chains(ctx: Context, track_index: int, device_index: int) -> str:
    """
    List the chains inside a rack device (Instrument Rack, Audio Effect Rack, Drum Rack).

    Parameters:
    - track_index: Index of the track
    - device_index: Index of the rack device on that track
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_rack_chains", {"track_index": track_index, "device_index": device_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting rack chains: {str(e)}"


@mcp.tool()
def set_rack_macro(ctx: Context, track_index: int, device_index: int, macro_index: int, value: float) -> str:
    """
    Set a macro knob value on a rack device.

    Parameters:
    - track_index: Index of the track
    - device_index: Index of the rack device
    - macro_index: Macro number (0 = Macro 1, 1 = Macro 2, ...)
    - value: Value within the macro's range
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_rack_macro", {
            "track_index": track_index, "device_index": device_index,
            "macro_index": macro_index, "value": value
        })
        return f"Set '{result.get('macro_name')}' to {result.get('value'):.4f}"
    except Exception as e:
        return f"Error setting rack macro: {str(e)}"


@mcp.tool()
def get_plugin_presets(ctx: Context, track_index: int, device_index: int) -> str:
    """
    List all presets available on a plugin device.

    Parameters:
    - track_index: Index of the track
    - device_index: Index of the plugin device on that track
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_plugin_presets", {"track_index": track_index, "device_index": device_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting plugin presets: {str(e)}"


@mcp.tool()
def set_plugin_preset(ctx: Context, track_index: int, device_index: int, preset_index: int) -> str:
    """
    Select a preset on a plugin device by index. Use get_plugin_presets to see available presets.

    Parameters:
    - track_index: Index of the track
    - device_index: Index of the plugin device
    - preset_index: Index of the preset to select
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_plugin_preset", {
            "track_index": track_index, "device_index": device_index, "preset_index": preset_index
        })
        return f"Plugin preset set to index {result.get('selected_preset_index')}"
    except Exception as e:
        return f"Error setting plugin preset: {str(e)}"


@mcp.tool()
def set_song_scale(
    ctx: Context,
    root_note: int = None,
    scale_name: str = None,
    scale_mode: bool = None,
) -> str:
    """
    Set the global key and scale for the session (Live 12+).
    Root notes: 0=C, 1=C#, 2=D, 3=D#, 4=E, 5=F, 6=F#, 7=G, 8=G#, 9=A, 10=A#, 11=B.

    Parameters:
    - root_note: Root note as integer 0–11
    - scale_name: Scale name string (e.g. 'Major', 'Minor', 'Dorian')
    - scale_mode: True to enable scale highlighting in clips
    """
    try:
        ableton = get_ableton_connection()
        params = {}
        if root_note is not None:
            params["root_note"] = root_note
        if scale_name is not None:
            params["scale_name"] = scale_name
        if scale_mode is not None:
            params["scale_mode"] = scale_mode
        result = ableton.send_command("set_song_scale", params)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error setting song scale: {str(e)}"


# Main execution
def main():
    """Run the MCP server"""
    mcp.run()

if __name__ == "__main__":
    main()
