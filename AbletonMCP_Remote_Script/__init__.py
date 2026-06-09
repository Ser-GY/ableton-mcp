# AbletonMCP/init.py
from __future__ import absolute_import, print_function, unicode_literals

from _Framework.ControlSurface import ControlSurface
import os
import socket
import json
import threading
import time
import traceback

# Change queue import for Python 2
try:
    import Queue as queue  # Python 2
except ImportError:
    import queue  # Python 3

# Constants for socket communication
DEFAULT_PORT = 9877
HOST = "127.0.0.1"  # localhost only — not exposed to the network
MAX_BUFFER_SIZE = 10 * 1024 * 1024  # 10 MB — prevents unbounded buffer growth

def create_instance(c_instance):
    """Create and return the AbletonMCP script instance"""
    return AbletonMCP(c_instance)

class AbletonMCP(ControlSurface):
    """AbletonMCP Remote Script for Ableton Live"""
    
    def __init__(self, c_instance):
        """Initialize the control surface"""
        ControlSurface.__init__(self, c_instance)
        self.log_message("AbletonMCP Remote Script initializing...")
        
        # Socket server for communication
        self.server = None
        self.client_threads = []
        self.server_thread = None
        self.running = False
        
        # Cache the song reference for easier access
        self._song = self.song()
        
        # Start the socket server
        self.start_server()
        
        self.log_message("AbletonMCP initialized")
        
        # Show a message in Ableton
        self.show_message("AbletonMCP: Listening for commands on port " + str(DEFAULT_PORT))
    
    def disconnect(self):
        """Called when Ableton closes or the control surface is removed"""
        self.log_message("AbletonMCP disconnecting...")
        self.running = False
        
        # Stop the server
        if self.server:
            try:
                self.server.close()
            except:
                pass
        
        # Wait for the server thread to exit
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(1.0)
            
        # Clean up any client threads
        for client_thread in self.client_threads[:]:
            if client_thread.is_alive():
                # We don't join them as they might be stuck
                self.log_message("Client thread still alive during disconnect")
        
        ControlSurface.disconnect(self)
        self.log_message("AbletonMCP disconnected")
    
    def start_server(self):
        """Start the socket server in a separate thread"""
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((HOST, DEFAULT_PORT))
            self.server.listen(5)  # Allow up to 5 pending connections
            
            self.running = True
            self.server_thread = threading.Thread(target=self._server_thread)
            self.server_thread.daemon = True
            self.server_thread.start()
            
            self.log_message("Server started on port " + str(DEFAULT_PORT))
        except Exception as e:
            self.log_message("Error starting server: " + str(e))
            self.show_message("AbletonMCP: Error starting server - " + str(e))
    
    def _server_thread(self):
        """Server thread implementation - handles client connections"""
        try:
            self.log_message("Server thread started")
            # Set a timeout to allow regular checking of running flag
            self.server.settimeout(1.0)
            
            while self.running:
                try:
                    # Accept connections with timeout
                    client, address = self.server.accept()
                    self.log_message("Connection accepted from " + str(address))
                    self.show_message("AbletonMCP: Client connected")
                    
                    # Handle client in a separate thread
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client,)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                    # Keep track of client threads
                    self.client_threads.append(client_thread)
                    
                    # Clean up finished client threads
                    self.client_threads = [t for t in self.client_threads if t.is_alive()]
                    
                except socket.timeout:
                    # No connection yet, just continue
                    continue
                except Exception as e:
                    if self.running:  # Only log if still running
                        self.log_message("Server accept error: " + str(e))
                    time.sleep(0.5)
            
            self.log_message("Server thread stopped")
        except Exception as e:
            self.log_message("Server thread error: " + str(e))
    
    def _handle_client(self, client):
        """Handle communication with a connected client"""
        self.log_message("Client handler started")
        client.settimeout(None)  # No timeout for client socket
        buffer = ''  # Changed from b'' to '' for Python 2
        
        try:
            while self.running:
                try:
                    # Receive data
                    data = client.recv(8192)
                    
                    if not data:
                        # Client disconnected
                        self.log_message("Client disconnected")
                        break
                    
                    # Accumulate data in buffer with explicit encoding/decoding
                    try:
                        # Python 3: data is bytes, decode to string
                        buffer += data.decode('utf-8')
                    except AttributeError:
                        # Python 2: data is already string
                        buffer += data

                    # Guard against unbounded memory growth from malformed/oversized payloads
                    if len(buffer) > MAX_BUFFER_SIZE:
                        self.log_message("Buffer exceeded max size, closing connection")
                        break

                    try:
                        # Try to parse command from buffer
                        command = json.loads(buffer)  # Removed decode('utf-8')
                        buffer = ''  # Clear buffer after successful parse
                        
                        self.log_message("Received command: " + str(command.get("type", "unknown")))
                        
                        # Process the command and get response
                        response = self._process_command(command)
                        
                        # Send the response with explicit encoding
                        try:
                            # Python 3: encode string to bytes
                            client.sendall(json.dumps(response).encode('utf-8'))
                        except AttributeError:
                            # Python 2: string is already bytes
                            client.sendall(json.dumps(response))
                    except ValueError:
                        # Incomplete data, wait for more
                        continue
                        
                except Exception as e:
                    self.log_message("Error handling client data: " + str(e))
                    self.log_message(traceback.format_exc())
                    
                    # Send error response if possible
                    error_response = {
                        "status": "error",
                        "message": str(e)
                    }
                    try:
                        # Python 3: encode string to bytes
                        client.sendall(json.dumps(error_response).encode('utf-8'))
                    except AttributeError:
                        # Python 2: string is already bytes
                        client.sendall(json.dumps(error_response))
                    except:
                        # If we can't send the error, the connection is probably dead
                        break
                    
                    # For serious errors, break the loop
                    if not isinstance(e, ValueError):
                        break
        except Exception as e:
            self.log_message("Error in client handler: " + str(e))
        finally:
            try:
                client.close()
            except:
                pass
            self.log_message("Client handler stopped")
    
    def _process_command(self, command):
        """Process a command from the client and return a response"""
        command_type = command.get("type", "")
        params = command.get("params", {})
        
        # Initialize response
        response = {
            "status": "success",
            "result": {}
        }
        
        try:
            # Route the command to the appropriate handler
            if command_type == "get_session_info":
                response["result"] = self._get_session_info()
            elif command_type == "get_track_info":
                track_index = params.get("track_index", 0)
                response["result"] = self._get_track_info(track_index)
            # Commands that modify Live's state should be scheduled on the main thread
            elif command_type in ["create_midi_track", "set_track_name",
                                 "create_clip", "create_audio_clip", "add_notes_to_clip", "set_clip_name",
                                 "set_tempo", "fire_clip", "stop_clip",
                                 "start_playback", "stop_playback", "load_browser_item",
                                 # Arrangement view – must run on the main thread
                                 "switch_to_arrangement_view", "set_current_song_time",
                                 "duplicate_session_clip_to_arrangement",
                                 # Track management
                                 "create_audio_track", "delete_track", "duplicate_track", "create_return_track",
                                 # Track mixer controls
                                 "set_track_volume", "set_track_pan", "set_track_mute",
                                 "set_track_solo", "set_track_arm", "set_track_color",
                                 # Device control
                                 "set_device_parameter",
                                 # Clip operations
                                 "delete_clip", "delete_notes_from_clip", "set_clip_loop",
                                 "set_clip_pitch", "set_clip_gain", "set_clip_warp_mode",
                                 "set_clip_signature", "duplicate_clip_in_session",
                                 # Scene operations
                                 "fire_scene", "create_scene", "delete_scene", "duplicate_scene",
                                 "set_scene_name", "set_scene_tempo",
                                 # Transport & recording
                                 "set_time_signature", "set_loop_points", "continue_playback",
                                 "set_metronome", "set_record_mode", "set_session_record",
                                 "capture_midi", "undo", "redo", "tap_tempo", "jump_to_cue",
                                 "stop_all_clips",
                                 # Routing
                                 "set_track_input_routing", "set_track_output_routing", "set_track_monitoring",
                                 "set_send_amount",
                                 # Master & return tracks
                                 "set_master_volume", "set_master_pan", "set_crossfader",
                                 "set_return_track_name", "set_return_track_volume",
                                 "set_return_track_pan", "set_return_track_mute",
                                 # Advanced
                                 "set_rack_macro", "set_plugin_preset", "set_song_scale"]:
                # Use a thread-safe approach with a response queue
                response_queue = queue.Queue()
                
                # Define a function to execute on the main thread
                def main_thread_task():
                    try:
                        result = None
                        if command_type == "create_midi_track":
                            index = params.get("index", -1)
                            result = self._create_midi_track(index)
                        elif command_type == "set_track_name":
                            track_index = params.get("track_index", 0)
                            name = params.get("name", "")
                            result = self._set_track_name(track_index, name)
                        elif command_type == "create_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            length = params.get("length", 4.0)
                            result = self._create_clip(track_index, clip_index, length)
                        elif command_type == "create_audio_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            path = params.get("path", "")
                            result = self._create_audio_clip(track_index, clip_index, path)
                        elif command_type == "add_notes_to_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            notes = params.get("notes", [])
                            result = self._add_notes_to_clip(track_index, clip_index, notes)
                        elif command_type == "set_clip_name":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            name = params.get("name", "")
                            result = self._set_clip_name(track_index, clip_index, name)
                        elif command_type == "set_tempo":
                            tempo = params.get("tempo", 120.0)
                            result = self._set_tempo(tempo)
                        elif command_type == "fire_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._fire_clip(track_index, clip_index)
                        elif command_type == "stop_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._stop_clip(track_index, clip_index)
                        elif command_type == "start_playback":
                            result = self._start_playback()
                        elif command_type == "stop_playback":
                            result = self._stop_playback()
                        elif command_type == "load_instrument_or_effect":
                            track_index = params.get("track_index", 0)
                            uri = params.get("uri", "")
                            result = self._load_instrument_or_effect(track_index, uri)
                        elif command_type == "load_browser_item":
                            track_index = params.get("track_index", 0)
                            item_uri = params.get("item_uri", "")
                            result = self._load_browser_item(track_index, item_uri)
                        # ── Arrangement view commands ──────────────────────────────
                        elif command_type == "switch_to_arrangement_view":
                            result = self._switch_to_arrangement_view()
                        elif command_type == "set_current_song_time":
                            time_val = params.get("time", 0.0)
                            result = self._set_current_song_time(time_val)
                        elif command_type == "duplicate_session_clip_to_arrangement":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            destination_time = params.get("destination_time", 0.0)
                            result = self._duplicate_session_clip_to_arrangement(
                                track_index, clip_index, destination_time)
                        # ── Track management ──────────────────────────────────────
                        elif command_type == "create_audio_track":
                            result = self._create_audio_track(params.get("index", -1))
                        elif command_type == "delete_track":
                            result = self._delete_track(params.get("track_index", 0))
                        elif command_type == "duplicate_track":
                            result = self._duplicate_track(params.get("track_index", 0))
                        elif command_type == "create_return_track":
                            result = self._create_return_track()
                        # ── Track mixer controls ───────────────────────────────────
                        elif command_type == "set_track_volume":
                            result = self._set_track_volume(params.get("track_index", 0), params.get("volume", 0.85))
                        elif command_type == "set_track_pan":
                            result = self._set_track_pan(params.get("track_index", 0), params.get("pan", 0.0))
                        elif command_type == "set_track_mute":
                            result = self._set_track_mute(params.get("track_index", 0), params.get("mute", False))
                        elif command_type == "set_track_solo":
                            result = self._set_track_solo(params.get("track_index", 0), params.get("solo", False))
                        elif command_type == "set_track_arm":
                            result = self._set_track_arm(params.get("track_index", 0), params.get("arm", False))
                        elif command_type == "set_track_color":
                            result = self._set_track_color(params.get("track_index", 0), params.get("color", 0))
                        # ── Device control ────────────────────────────────────────
                        elif command_type == "set_device_parameter":
                            result = self._set_device_parameter(
                                params.get("track_index", 0), params.get("device_index", 0),
                                params.get("parameter_index", 0), params.get("value", 0.0))
                        # ── Clip operations ───────────────────────────────────────
                        elif command_type == "delete_clip":
                            result = self._delete_clip(params.get("track_index", 0), params.get("clip_index", 0))
                        elif command_type == "delete_notes_from_clip":
                            result = self._delete_notes_from_clip(
                                params.get("track_index", 0), params.get("clip_index", 0),
                                params.get("from_time", 0.0), params.get("time_span", 1.0),
                                params.get("from_pitch", 0), params.get("pitch_span", 128))
                        elif command_type == "set_clip_loop":
                            result = self._set_clip_loop(
                                params.get("track_index", 0), params.get("clip_index", 0),
                                params.get("looping", True), params.get("loop_start", None),
                                params.get("loop_end", None))
                        elif command_type == "set_clip_pitch":
                            result = self._set_clip_pitch(
                                params.get("track_index", 0), params.get("clip_index", 0),
                                params.get("coarse", None), params.get("fine", None))
                        elif command_type == "set_clip_gain":
                            result = self._set_clip_gain(
                                params.get("track_index", 0), params.get("clip_index", 0),
                                params.get("gain", 1.0))
                        elif command_type == "set_clip_warp_mode":
                            result = self._set_clip_warp_mode(
                                params.get("track_index", 0), params.get("clip_index", 0),
                                params.get("warp_mode", None), params.get("warping", None))
                        elif command_type == "set_clip_signature":
                            result = self._set_clip_signature(
                                params.get("track_index", 0), params.get("clip_index", 0),
                                params.get("numerator", 4), params.get("denominator", 4))
                        elif command_type == "duplicate_clip_in_session":
                            result = self._duplicate_clip_in_session(
                                params.get("track_index", 0),
                                params.get("src_clip_index", 0), params.get("dst_clip_index", 1))
                        # ── Scene operations ──────────────────────────────────────
                        elif command_type == "fire_scene":
                            result = self._fire_scene(params.get("scene_index", 0))
                        elif command_type == "create_scene":
                            result = self._create_scene(params.get("index", -1))
                        elif command_type == "delete_scene":
                            result = self._delete_scene(params.get("scene_index", 0))
                        elif command_type == "duplicate_scene":
                            result = self._duplicate_scene(params.get("scene_index", 0))
                        elif command_type == "set_scene_name":
                            result = self._set_scene_name(params.get("scene_index", 0), params.get("name", ""))
                        elif command_type == "set_scene_tempo":
                            result = self._set_scene_tempo(
                                params.get("scene_index", 0), params.get("tempo", 120.0),
                                params.get("enabled", True))
                        # ── Transport & recording ─────────────────────────────────
                        elif command_type == "set_time_signature":
                            result = self._set_time_signature(params.get("numerator", 4), params.get("denominator", 4))
                        elif command_type == "set_loop_points":
                            result = self._set_loop_points(
                                params.get("loop_on", True),
                                params.get("loop_start", None), params.get("loop_length", None))
                        elif command_type == "continue_playback":
                            result = self._continue_playback()
                        elif command_type == "set_metronome":
                            result = self._set_metronome(params.get("enabled", True))
                        elif command_type == "set_record_mode":
                            result = self._set_record_mode(params.get("enabled", False))
                        elif command_type == "set_session_record":
                            result = self._set_session_record(params.get("enabled", False))
                        elif command_type == "capture_midi":
                            result = self._capture_midi()
                        elif command_type == "undo":
                            result = self._undo()
                        elif command_type == "redo":
                            result = self._redo()
                        elif command_type == "tap_tempo":
                            result = self._tap_tempo()
                        elif command_type == "jump_to_cue":
                            result = self._jump_to_cue(params.get("direction", "next"))
                        elif command_type == "stop_all_clips":
                            result = self._stop_all_clips(params.get("quantized", True))
                        # ── Routing ───────────────────────────────────────────────
                        elif command_type == "set_track_input_routing":
                            result = self._set_track_input_routing(
                                params.get("track_index", 0), params.get("routing", ""))
                        elif command_type == "set_track_output_routing":
                            result = self._set_track_output_routing(
                                params.get("track_index", 0), params.get("routing", ""))
                        elif command_type == "set_track_monitoring":
                            result = self._set_track_monitoring(
                                params.get("track_index", 0), params.get("monitoring_state", 1))
                        elif command_type == "set_send_amount":
                            result = self._set_send_amount(
                                params.get("track_index", 0),
                                params.get("send_index", 0), params.get("value", 0.0))
                        # ── Master & return tracks ────────────────────────────────
                        elif command_type == "set_master_volume":
                            result = self._set_master_volume(params.get("volume", 0.85))
                        elif command_type == "set_master_pan":
                            result = self._set_master_pan(params.get("pan", 0.0))
                        elif command_type == "set_crossfader":
                            result = self._set_crossfader(params.get("value", 0.0))
                        elif command_type == "set_return_track_name":
                            result = self._set_return_track_name(
                                params.get("track_index", 0), params.get("name", ""))
                        elif command_type == "set_return_track_volume":
                            result = self._set_return_track_volume(
                                params.get("track_index", 0), params.get("volume", 0.85))
                        elif command_type == "set_return_track_pan":
                            result = self._set_return_track_pan(
                                params.get("track_index", 0), params.get("pan", 0.0))
                        elif command_type == "set_return_track_mute":
                            result = self._set_return_track_mute(
                                params.get("track_index", 0), params.get("mute", False))
                        # ── Advanced ──────────────────────────────────────────────
                        elif command_type == "set_rack_macro":
                            result = self._set_rack_macro(
                                params.get("track_index", 0), params.get("device_index", 0),
                                params.get("macro_index", 0), params.get("value", 0.0))
                        elif command_type == "set_plugin_preset":
                            result = self._set_plugin_preset(
                                params.get("track_index", 0), params.get("device_index", 0),
                                params.get("preset_index", 0))
                        elif command_type == "set_song_scale":
                            result = self._set_song_scale(
                                params.get("root_note", None),
                                params.get("scale_name", None),
                                params.get("scale_mode", None))

                        # Put the result in the queue
                        response_queue.put({"status": "success", "result": result})
                    except Exception as e:
                        self.log_message("Error in main thread task: " + str(e))
                        self.log_message(traceback.format_exc())
                        response_queue.put({"status": "error", "message": str(e)})
                
                # Schedule the task to run on the main thread
                try:
                    self.schedule_message(0, main_thread_task)
                except AssertionError:
                    # If we're already on the main thread, execute directly
                    main_thread_task()
                
                # Wait for the response with a timeout. Some commands (notably
                # create_audio_clip, which decodes/imports the audio file on
                # the main thread) can take longer than the default 10s on
                # larger files — give them more headroom.
                long_running_commands = {"create_audio_clip": 60.0}
                queue_timeout = long_running_commands.get(command_type, 10.0)
                try:
                    task_response = response_queue.get(timeout=queue_timeout)
                    if task_response.get("status") == "error":
                        response["status"] = "error"
                        response["message"] = task_response.get("message", "Unknown error")
                    else:
                        response["result"] = task_response.get("result", {})
                except queue.Empty:
                    response["status"] = "error"
                    response["message"] = "Timeout waiting for operation to complete"
            elif command_type == "get_browser_item":
                uri = params.get("uri", None)
                path = params.get("path", None)
                response["result"] = self._get_browser_item(uri, path)
            elif command_type == "get_browser_categories":
                category_type = params.get("category_type", "all")
                response["result"] = self._get_browser_categories(category_type)
            elif command_type == "get_browser_items":
                path = params.get("path", "")
                item_type = params.get("item_type", "all")
                response["result"] = self._get_browser_items(path, item_type)
            # Add the new browser commands
            elif command_type == "get_browser_tree":
                category_type = params.get("category_type", "all")
                response["result"] = self.get_browser_tree(category_type)
            elif command_type == "get_browser_items_at_path":
                path = params.get("path", "")
                response["result"] = self.get_browser_items_at_path(path)
            # Read-only arrangement command – no main-thread scheduling required
            elif command_type == "get_arrangement_clips":
                track_index = params.get("track_index", 0)
                response["result"] = self._get_arrangement_clips(track_index)
            # Read-only extended commands
            elif command_type == "get_device_parameters":
                response["result"] = self._get_device_parameters(
                    params.get("track_index", 0), params.get("device_index", 0))
            elif command_type == "get_clip_notes":
                response["result"] = self._get_clip_notes(
                    params.get("track_index", 0), params.get("clip_index", 0))
            elif command_type == "get_return_track_info":
                response["result"] = self._get_return_track_info(params.get("track_index", 0))
            elif command_type == "get_plugin_presets":
                response["result"] = self._get_plugin_presets(
                    params.get("track_index", 0), params.get("device_index", 0))
            elif command_type == "get_rack_chains":
                response["result"] = self._get_rack_chains(
                    params.get("track_index", 0), params.get("device_index", 0))
            else:
                response["status"] = "error"
                response["message"] = "Unknown command: " + command_type
        except Exception as e:
            self.log_message("Error processing command: " + str(e))
            self.log_message(traceback.format_exc())
            response["status"] = "error"
            response["message"] = str(e)
        
        return response
    
    # Command implementations
    
    def _safe_song_property(self, attr, cast, default):
        """Read self._song.<attr> with cast, returning default on common failures.
        Catches only narrow exceptions so genuine bugs still surface."""
        try:
            return cast(getattr(self._song, attr))
        except (AttributeError, TypeError, ValueError):
            return default

    def _get_session_info(self):
        """Get information about the current session"""
        try:
            result = {
                "tempo": self._song.tempo,
                "signature_numerator": self._song.signature_numerator,
                "signature_denominator": self._song.signature_denominator,
                "track_count": len(self._song.tracks),
                "return_track_count": len(self._song.return_tracks),
                "master_track": {
                    "name": "Master",
                    "volume": self._song.master_track.mixer_device.volume.value,
                    "panning": self._song.master_track.mixer_device.panning.value
                },
                # Transport / playback state — lets clients render a live
                # playhead without polling separately. Each property is read
                # via _safe_song_property so an attribute missing on a given
                # Live version falls back to its default rather than breaking
                # the response shape.
                "is_playing":        self._safe_song_property("is_playing",        bool,  False),
                "current_song_time": self._safe_song_property("current_song_time", float, 0.0),
                "song_length":       self._safe_song_property("song_length",       float, 0.0),
                "loop":              self._safe_song_property("loop",              bool,  False),
                "loop_start":        self._safe_song_property("loop_start",        float, 0.0),
                "loop_length":       self._safe_song_property("loop_length",       float, 0.0),
            }
            return result
        except Exception as e:
            self.log_message("Error getting session info: " + str(e))
            raise
    
    def _get_track_info(self, track_index):
        """Get information about a track"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            # Get clip slots
            clip_slots = []
            for slot_index, slot in enumerate(track.clip_slots):
                clip_info = None
                if slot.has_clip:
                    clip = slot.clip
                    clip_info = {
                        "name": clip.name,
                        "length": clip.length,
                        "is_playing": clip.is_playing,
                        "is_recording": clip.is_recording
                    }
                
                clip_slots.append({
                    "index": slot_index,
                    "has_clip": slot.has_clip,
                    "clip": clip_info
                })
            
            # Get devices
            devices = []
            for device_index, device in enumerate(track.devices):
                devices.append({
                    "index": device_index,
                    "name": device.name,
                    "class_name": device.class_name,
                    "type": self._get_device_type(device)
                })
            
            result = {
                "index": track_index,
                "name": track.name,
                "is_audio_track": track.has_audio_input,
                "is_midi_track": track.has_midi_input,
                "mute": track.mute,
                "solo": track.solo,
                "arm": track.arm,
                "volume": track.mixer_device.volume.value,
                "panning": track.mixer_device.panning.value,
                "clip_slots": clip_slots,
                "devices": devices
            }
            return result
        except Exception as e:
            self.log_message("Error getting track info: " + str(e))
            raise
    
    def _create_midi_track(self, index):
        """Create a new MIDI track at the specified index"""
        try:
            # Create the track
            self._song.create_midi_track(index)
            
            # Get the new track
            new_track_index = len(self._song.tracks) - 1 if index == -1 else index
            new_track = self._song.tracks[new_track_index]
            
            result = {
                "index": new_track_index,
                "name": new_track.name
            }
            return result
        except Exception as e:
            self.log_message("Error creating MIDI track: " + str(e))
            raise
    
    
    def _set_track_name(self, track_index, name):
        """Set the name of a track"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            # Set the name
            track = self._song.tracks[track_index]
            track.name = name
            
            result = {
                "name": track.name
            }
            return result
        except Exception as e:
            self.log_message("Error setting track name: " + str(e))
            raise
    
    def _create_clip(self, track_index, clip_index, length):
        """Create a new MIDI clip in the specified track and clip slot"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            # Check if the clip slot already has a clip
            if clip_slot.has_clip:
                raise Exception("Clip slot already has a clip")
            
            # Create the clip
            clip_slot.create_clip(length)
            
            result = {
                "name": clip_slot.clip.name,
                "length": clip_slot.clip.length
            }
            return result
        except Exception as e:
            self.log_message("Error creating clip: " + str(e))
            raise

    def _create_audio_clip(self, track_index, clip_index, path):
        """Create an audio clip in the specified audio track clip slot by importing a file.

        Requires Ableton Live 12.0.5 or newer (the underlying
        ClipSlot.create_audio_clip Live API was introduced in 12.0.5 — it is
        not available in earlier 12.0.x releases).
        """
        try:
            if not path:
                raise ValueError("Audio file path is required")

            if not os.path.isabs(path):
                raise ValueError("Audio file path must be absolute (got: %s)" % path)

            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")

            track = self._song.tracks[track_index]

            # Must be an audio track. Audio tracks expose audio input; MIDI
            # tracks don't. Reject MIDI / return tracks up front so the caller
            # gets a clear error instead of a Live API exception.
            if getattr(track, "has_midi_input", False) or not getattr(track, "has_audio_input", True):
                raise ValueError("Track %d is not an audio track" % track_index)

            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")

            clip_slot = track.clip_slots[clip_index]

            if clip_slot.has_clip:
                raise Exception("Clip slot already has a clip")

            if not hasattr(clip_slot, "create_audio_clip"):
                raise Exception(
                    "ClipSlot.create_audio_clip is unavailable in this Ableton Live "
                    "version. Requires Live 12.0.5 or newer."
                )

            clip_slot.create_audio_clip(path)

            result = {
                "name": clip_slot.clip.name,
                "length": clip_slot.clip.length,
                "is_audio_clip": clip_slot.clip.is_audio_clip
            }
            return result
        except Exception as e:
            self.log_message("Error creating audio clip: " + str(e))
            raise

    def _add_notes_to_clip(self, track_index, clip_index, notes):
        """Add MIDI notes to a clip"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            if not clip_slot.has_clip:
                raise Exception("No clip in slot")
            
            clip = clip_slot.clip
            
            # Convert note data to Live's format
            live_notes = []
            for note in notes:
                pitch = note.get("pitch", 60)
                start_time = note.get("start_time", 0.0)
                duration = note.get("duration", 0.25)
                velocity = note.get("velocity", 100)
                mute = note.get("mute", False)
                
                live_notes.append((pitch, start_time, duration, velocity, mute))
            
            # Add the notes
            clip.set_notes(tuple(live_notes))
            
            result = {
                "note_count": len(notes)
            }
            return result
        except Exception as e:
            self.log_message("Error adding notes to clip: " + str(e))
            raise
    
    def _set_clip_name(self, track_index, clip_index, name):
        """Set the name of a clip"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            if not clip_slot.has_clip:
                raise Exception("No clip in slot")
            
            clip = clip_slot.clip
            clip.name = name
            
            result = {
                "name": clip.name
            }
            return result
        except Exception as e:
            self.log_message("Error setting clip name: " + str(e))
            raise
    
    def _set_tempo(self, tempo):
        """Set the tempo of the session"""
        try:
            self._song.tempo = tempo
            
            result = {
                "tempo": self._song.tempo
            }
            return result
        except Exception as e:
            self.log_message("Error setting tempo: " + str(e))
            raise
    
    def _fire_clip(self, track_index, clip_index):
        """Fire a clip"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            if not clip_slot.has_clip:
                raise Exception("No clip in slot")
            
            clip_slot.fire()
            
            result = {
                "fired": True
            }
            return result
        except Exception as e:
            self.log_message("Error firing clip: " + str(e))
            raise
    
    def _stop_clip(self, track_index, clip_index):
        """Stop a clip"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            clip_slot.stop()
            
            result = {
                "stopped": True
            }
            return result
        except Exception as e:
            self.log_message("Error stopping clip: " + str(e))
            raise
    
    
    def _start_playback(self):
        """Start playing the session"""
        try:
            self._song.start_playing()
            
            result = {
                "playing": self._song.is_playing
            }
            return result
        except Exception as e:
            self.log_message("Error starting playback: " + str(e))
            raise
    
    def _stop_playback(self):
        """Stop playing the session"""
        try:
            self._song.stop_playing()
            
            result = {
                "playing": self._song.is_playing
            }
            return result
        except Exception as e:
            self.log_message("Error stopping playback: " + str(e))
            raise
    
    # ── Arrangement view implementations ──────────────────────────────────────

    def _switch_to_arrangement_view(self):
        """Switch Ableton's main window to the Arrangement view"""
        try:
            self.application().view.show_view("Arranger")
            return {"view": "Arranger"}
        except Exception as e:
            self.log_message("Error switching to arrangement view: " + str(e))
            raise

    def _set_current_song_time(self, time_val):
        """Move the arrangement playhead to a position in beats"""
        try:
            self._song.current_song_time = float(time_val)
            return {"current_song_time": self._song.current_song_time}
        except Exception as e:
            self.log_message("Error setting current song time: " + str(e))
            raise

    def _get_arrangement_clips(self, track_index):
        """Return all clips placed in the Arrangement timeline for a track.

        Each clip dict contains:
          name, start_time, end_time, length, color,
          is_midi_clip, is_audio_clip, is_playing
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")

            track = self._song.tracks[track_index]
            clips = []

            # track.arrangement_clips is available in Live 11 / 12
            for clip in track.arrangement_clips:
                clips.append({
                    "name": clip.name,
                    "start_time": clip.start_time,
                    "end_time": clip.end_time,
                    "length": clip.length,
                    "color": clip.color,
                    "is_midi_clip": clip.is_midi_clip,
                    "is_audio_clip": clip.is_audio_clip,
                    "is_playing": clip.is_playing
                })

            return {
                "track_index": track_index,
                "track_name": track.name,
                "clip_count": len(clips),
                "clips": clips
            }
        except Exception as e:
            self.log_message("Error getting arrangement clips: " + str(e))
            raise

    def _duplicate_session_clip_to_arrangement(self, track_index, clip_index, destination_time):
        """Copy a Session-view clip into the Arrangement timeline.

        Uses the real Live API:
          track.duplicate_clip_to_arrangement(clip, destination_time)

        Available in Live 11 / 12.  destination_time is in beats from the
        start of the arrangement.
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")

            track = self._song.tracks[track_index]

            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip slot index out of range")

            clip_slot = track.clip_slots[clip_index]

            if not clip_slot.has_clip:
                raise Exception(
                    "No clip in slot " + str(clip_index) +
                    " on track " + str(track_index)
                )

            clip = clip_slot.clip

            # Duplicate to arrangement at the requested beat position
            track.duplicate_clip_to_arrangement(clip, float(destination_time))

            return {
                "success": True,
                "track_index": track_index,
                "track_name": track.name,
                "clip_name": clip.name,
                "destination_time": destination_time
            }
        except Exception as e:
            self.log_message("Error duplicating clip to arrangement: " + str(e))
            raise

    # ── Browser implementations ───────────────────────────────────────────────

    def _get_browser_item(self, uri, path):
        """Get a browser item by URI or path"""
        try:
            # Access the application's browser instance instead of creating a new one
            app = self.application()
            if not app:
                raise RuntimeError("Could not access Live application")
                
            result = {
                "uri": uri,
                "path": path,
                "found": False
            }
            
            # Try to find by URI first if provided
            if uri:
                item = self._find_browser_item_by_uri(app.browser, uri)
                if item:
                    result["found"] = True
                    result["item"] = {
                        "name": item.name,
                        "is_folder": item.is_folder,
                        "is_device": item.is_device,
                        "is_loadable": item.is_loadable,
                        "uri": item.uri
                    }
                    return result
            
            # If URI not provided or not found, try by path
            if path:
                # Parse the path and navigate to the specified item
                path_parts = path.split("/")
                
                # Determine the root based on the first part
                current_item = None
                if path_parts[0].lower() == "instruments":
                    current_item = app.browser.instruments
                elif path_parts[0].lower() == "sounds":
                    current_item = app.browser.sounds
                elif path_parts[0].lower() == "drums":
                    current_item = app.browser.drums
                elif path_parts[0].lower() == "audio_effects":
                    current_item = app.browser.audio_effects
                elif path_parts[0].lower() == "midi_effects":
                    current_item = app.browser.midi_effects
                else:
                    # Default to instruments if not specified
                    current_item = app.browser.instruments
                    # Don't skip the first part in this case
                    path_parts = ["instruments"] + path_parts
                
                # Navigate through the path
                for i in range(1, len(path_parts)):
                    part = path_parts[i]
                    if not part:  # Skip empty parts
                        continue
                    
                    found = False
                    for child in current_item.children:
                        if child.name.lower() == part.lower():
                            current_item = child
                            found = True
                            break
                    
                    if not found:
                        result["error"] = "Path part '{0}' not found".format(part)
                        return result
                
                # Found the item
                result["found"] = True
                result["item"] = {
                    "name": current_item.name,
                    "is_folder": current_item.is_folder,
                    "is_device": current_item.is_device,
                    "is_loadable": current_item.is_loadable,
                    "uri": current_item.uri
                }
            
            return result
        except Exception as e:
            self.log_message("Error getting browser item: " + str(e))
            self.log_message(traceback.format_exc())
            raise   
    
    
    
    def _load_browser_item(self, track_index, item_uri):
        """Load a browser item onto a track by its URI"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            # Access the application's browser instance instead of creating a new one
            app = self.application()
            
            # Find the browser item by URI
            item = self._find_browser_item_by_uri(app.browser, item_uri)
            
            if not item:
                raise ValueError("Browser item with URI '{0}' not found".format(item_uri))
            
            # Select the track
            self._song.view.selected_track = track
            
            # Load the item
            app.browser.load_item(item)
            
            result = {
                "loaded": True,
                "item_name": item.name,
                "track_name": track.name,
                "uri": item_uri
            }
            return result
        except Exception as e:
            self.log_message("Error loading browser item: {0}".format(str(e)))
            self.log_message(traceback.format_exc())
            raise
    
    # Substring markers that point a URI at a likely root. If no marker
    # matches we fall back to the default order, so this is purely an
    # optimisation — never a correctness change.
    _URI_ROOT_HINTS = (
        ('plugins',       ('vst:', 'vst3:', 'au:', 'query:plugins', 'plugin#')),
        ('max_for_live',  ('max for live', 'maxforlive', 'm4l', 'query:max')),
        ('user_library',  ('user library', 'userlibrary', 'query:user library', 'query:user-library')),
        ('packs',         ('query:packs', '/packs/')),
        ('samples',       ('query:samples', 'sample:', '/samples/')),
        ('drums',         ('query:drums', '/drums/')),
        ('instruments',   ('query:instruments', '/instruments/')),
        ('sounds',        ('query:sounds', '/sounds/')),
        ('audio_effects', ('query:audio effects', 'audioeffects', '/audio_effects/')),
        ('midi_effects',  ('query:midi effects', 'midieffects', '/midi_effects/')),
    )

    def _order_roots_by_uri(self, roots, uri):
        """Reorder ``roots`` so the URI's likely root is walked first."""
        if not isinstance(uri, (bytes, str)) or not uri:
            return roots
        lowered = uri.lower()
        for attr, markers in self._URI_ROOT_HINTS:
            if any(m in lowered for m in markers):
                head = [(a, r) for (a, r) in roots if a == attr]
                tail = [(a, r) for (a, r) in roots if a != attr]
                return head + tail
        return roots

    def _find_browser_item_by_uri(self, browser_or_item, uri, max_depth=10, current_depth=0):
        """Find a browser item by its URI.

        Top-level lookups are memoised on ``self._uri_cache`` so repeated
        loads of the same URI don't re-walk the entire browser tree.
        """
        if current_depth == 0:
            cache = getattr(self, '_uri_cache', None)
            if cache is None:
                self._uri_cache = cache = {}
            if uri in cache:
                return cache[uri]
            result = self._walk_browser_for_uri(browser_or_item, uri, max_depth, 0)
            if result is not None:
                cache[uri] = result
            return result
        return self._walk_browser_for_uri(browser_or_item, uri, max_depth, current_depth)

    def _walk_browser_for_uri(self, browser_or_item, uri, max_depth, current_depth):
        """Recursive walk used by :py:meth:`_find_browser_item_by_uri`."""
        try:
            # Check if this is the item we're looking for
            if hasattr(browser_or_item, 'uri') and browser_or_item.uri == uri:
                return browser_or_item

            # Stop recursion if we've reached max depth
            if current_depth >= max_depth:
                return None

            # Check if this is a browser with root categories
            if hasattr(browser_or_item, 'instruments'):
                roots = [
                    ('instruments', browser_or_item.instruments),
                    ('sounds', browser_or_item.sounds),
                    ('drums', browser_or_item.drums),
                    ('audio_effects', browser_or_item.audio_effects),
                    ('midi_effects', browser_or_item.midi_effects),
                ]
                for extra_attr in ('plugins', 'max_for_live', 'user_library', 'packs', 'samples'):
                    if hasattr(browser_or_item, extra_attr):
                        try:
                            roots.append((extra_attr, getattr(browser_or_item, extra_attr)))
                        except (AttributeError, RuntimeError) as e:
                            self.log_message("Could not access browser.{0}: {1}".format(extra_attr, str(e)))

                for _attr, category in self._order_roots_by_uri(roots, uri):
                    item = self._find_browser_item_by_uri(category, uri, max_depth, current_depth + 1)
                    if item:
                        return item

                return None

            # Check if this item has children
            if hasattr(browser_or_item, 'children') and browser_or_item.children:
                for child in browser_or_item.children:
                    item = self._find_browser_item_by_uri(child, uri, max_depth, current_depth + 1)
                    if item:
                        return item

            return None
        except Exception as e:
            self.log_message("Error finding browser item by URI: {0}".format(str(e)))
            return None
    
    # Helper methods
    
    def _get_device_type(self, device):
        """Get the type of a device"""
        try:
            # Simple heuristic - in a real implementation you'd look at the device class
            if device.can_have_drum_pads:
                return "drum_machine"
            elif device.can_have_chains:
                return "rack"
            elif "instrument" in device.class_display_name.lower():
                return "instrument"
            elif "audio_effect" in device.class_name.lower():
                return "audio_effect"
            elif "midi_effect" in device.class_name.lower():
                return "midi_effect"
            else:
                return "unknown"
        except:
            return "unknown"
    
    def get_browser_tree(self, category_type="all"):
        """
        Get a simplified tree of browser categories.
        
        Args:
            category_type: Type of categories to get ('all', 'instruments', 'sounds', etc.)
            
        Returns:
            Dictionary with the browser tree structure
        """
        try:
            # Access the application's browser instance instead of creating a new one
            app = self.application()
            if not app:
                raise RuntimeError("Could not access Live application")
                
            # Check if browser is available
            if not hasattr(app, 'browser') or app.browser is None:
                raise RuntimeError("Browser is not available in the Live application")
            
            # Log available browser attributes to help diagnose issues
            browser_attrs = [attr for attr in dir(app.browser) if not attr.startswith('_')]
            self.log_message("Available browser attributes: {0}".format(browser_attrs))
            
            result = {
                "type": category_type,
                "categories": [],
                "available_categories": browser_attrs
            }
            
            # Helper function to process a browser item and its children
            def process_item(item, depth=0):
                if not item:
                    return None
                
                result = {
                    "name": item.name if hasattr(item, 'name') else "Unknown",
                    "is_folder": hasattr(item, 'children') and bool(item.children),
                    "is_device": hasattr(item, 'is_device') and item.is_device,
                    "is_loadable": hasattr(item, 'is_loadable') and item.is_loadable,
                    "uri": item.uri if hasattr(item, 'uri') else None,
                    "children": []
                }
                
                
                return result
            
            # Process based on category type and available attributes
            if (category_type == "all" or category_type == "instruments") and hasattr(app.browser, 'instruments'):
                try:
                    instruments = process_item(app.browser.instruments)
                    if instruments:
                        instruments["name"] = "Instruments"  # Ensure consistent naming
                        result["categories"].append(instruments)
                except Exception as e:
                    self.log_message("Error processing instruments: {0}".format(str(e)))
            
            if (category_type == "all" or category_type == "sounds") and hasattr(app.browser, 'sounds'):
                try:
                    sounds = process_item(app.browser.sounds)
                    if sounds:
                        sounds["name"] = "Sounds"  # Ensure consistent naming
                        result["categories"].append(sounds)
                except Exception as e:
                    self.log_message("Error processing sounds: {0}".format(str(e)))
            
            if (category_type == "all" or category_type == "drums") and hasattr(app.browser, 'drums'):
                try:
                    drums = process_item(app.browser.drums)
                    if drums:
                        drums["name"] = "Drums"  # Ensure consistent naming
                        result["categories"].append(drums)
                except Exception as e:
                    self.log_message("Error processing drums: {0}".format(str(e)))
            
            if (category_type == "all" or category_type == "audio_effects") and hasattr(app.browser, 'audio_effects'):
                try:
                    audio_effects = process_item(app.browser.audio_effects)
                    if audio_effects:
                        audio_effects["name"] = "Audio Effects"  # Ensure consistent naming
                        result["categories"].append(audio_effects)
                except Exception as e:
                    self.log_message("Error processing audio_effects: {0}".format(str(e)))
            
            if (category_type == "all" or category_type == "midi_effects") and hasattr(app.browser, 'midi_effects'):
                try:
                    midi_effects = process_item(app.browser.midi_effects)
                    if midi_effects:
                        midi_effects["name"] = "MIDI Effects"
                        result["categories"].append(midi_effects)
                except Exception as e:
                    self.log_message("Error processing midi_effects: {0}".format(str(e)))
            
            # Try to process other potentially available categories
            for attr in browser_attrs:
                if attr not in ['instruments', 'sounds', 'drums', 'audio_effects', 'midi_effects'] and \
                   (category_type == "all" or category_type == attr):
                    try:
                        item = getattr(app.browser, attr)
                        if hasattr(item, 'children') or hasattr(item, 'name'):
                            category = process_item(item)
                            if category:
                                category["name"] = attr.capitalize()
                                result["categories"].append(category)
                    except Exception as e:
                        self.log_message("Error processing {0}: {1}".format(attr, str(e)))
            
            self.log_message("Browser tree generated for {0} with {1} root categories".format(
                category_type, len(result['categories'])))
            return result
            
        except Exception as e:
            self.log_message("Error getting browser tree: {0}".format(str(e)))
            self.log_message(traceback.format_exc())
            raise
    
    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_track(self, track_index):
        if track_index < 0 or track_index >= len(self._song.tracks):
            raise IndexError("Track index out of range")
        return self._song.tracks[track_index]

    def _get_return_track(self, track_index):
        if track_index < 0 or track_index >= len(self._song.return_tracks):
            raise IndexError("Return track index out of range")
        return self._song.return_tracks[track_index]

    def _get_clip_slot(self, track_index, clip_index, require_clip=True):
        track = self._get_track(track_index)
        if clip_index < 0 or clip_index >= len(track.clip_slots):
            raise IndexError("Clip index out of range")
        slot = track.clip_slots[clip_index]
        if require_clip and not slot.has_clip:
            raise Exception("No clip in slot " + str(clip_index))
        return slot

    def _get_clip(self, track_index, clip_index):
        return self._get_clip_slot(track_index, clip_index, require_clip=True).clip

    # ── Track management ──────────────────────────────────────────────────────

    def _create_audio_track(self, index):
        try:
            self._song.create_audio_track(index)
            new_index = len(self._song.tracks) - 1 if index == -1 else index
            track = self._song.tracks[new_index]
            return {"index": new_index, "name": track.name}
        except Exception as e:
            self.log_message("Error creating audio track: " + str(e))
            raise

    def _delete_track(self, track_index):
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            self._song.delete_track(track_index)
            return {"deleted": True, "track_index": track_index}
        except Exception as e:
            self.log_message("Error deleting track: " + str(e))
            raise

    def _duplicate_track(self, track_index):
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            self._song.duplicate_track(track_index)
            return {"duplicated": True, "original_index": track_index, "new_index": track_index + 1}
        except Exception as e:
            self.log_message("Error duplicating track: " + str(e))
            raise

    def _create_return_track(self):
        try:
            self._song.create_return_track()
            new_index = len(self._song.return_tracks) - 1
            track = self._song.return_tracks[new_index]
            return {"index": new_index, "name": track.name}
        except Exception as e:
            self.log_message("Error creating return track: " + str(e))
            raise

    # ── Track mixer controls ──────────────────────────────────────────────────

    def _set_track_volume(self, track_index, volume):
        try:
            track = self._get_track(track_index)
            track.mixer_device.volume.value = float(volume)
            return {"volume": track.mixer_device.volume.value}
        except Exception as e:
            self.log_message("Error setting track volume: " + str(e))
            raise

    def _set_track_pan(self, track_index, pan):
        try:
            track = self._get_track(track_index)
            track.mixer_device.panning.value = float(pan)
            return {"panning": track.mixer_device.panning.value}
        except Exception as e:
            self.log_message("Error setting track pan: " + str(e))
            raise

    def _set_track_mute(self, track_index, mute):
        try:
            track = self._get_track(track_index)
            track.mute = bool(mute)
            return {"mute": track.mute}
        except Exception as e:
            self.log_message("Error setting track mute: " + str(e))
            raise

    def _set_track_solo(self, track_index, solo):
        try:
            track = self._get_track(track_index)
            track.solo = bool(solo)
            return {"solo": track.solo}
        except Exception as e:
            self.log_message("Error setting track solo: " + str(e))
            raise

    def _set_track_arm(self, track_index, arm):
        try:
            track = self._get_track(track_index)
            if not getattr(track, 'can_be_armed', False):
                raise Exception("Track cannot be armed")
            track.arm = bool(arm)
            return {"arm": track.arm}
        except Exception as e:
            self.log_message("Error setting track arm: " + str(e))
            raise

    def _set_track_color(self, track_index, color):
        try:
            track = self._get_track(track_index)
            track.color = int(color)
            return {"color": track.color}
        except Exception as e:
            self.log_message("Error setting track color: " + str(e))
            raise

    # ── Device control ────────────────────────────────────────────────────────

    def _get_device_parameters(self, track_index, device_index):
        try:
            track = self._get_track(track_index)
            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            device = track.devices[device_index]
            params = []
            for i, param in enumerate(device.parameters):
                try:
                    value_items = list(param.value_items) if getattr(param, 'is_quantized', False) else []
                except:
                    value_items = []
                params.append({
                    "index": i,
                    "name": param.name,
                    "value": float(param.value),
                    "min": float(param.min),
                    "max": float(param.max),
                    "default_value": float(getattr(param, 'default_value', param.min)),
                    "is_quantized": bool(getattr(param, 'is_quantized', False)),
                    "value_items": value_items
                })
            return {"device_name": device.name, "parameter_count": len(params), "parameters": params}
        except Exception as e:
            self.log_message("Error getting device parameters: " + str(e))
            raise

    def _set_device_parameter(self, track_index, device_index, parameter_index, value):
        try:
            track = self._get_track(track_index)
            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            device = track.devices[device_index]
            if parameter_index < 0 or parameter_index >= len(device.parameters):
                raise IndexError("Parameter index out of range")
            param = device.parameters[parameter_index]
            param.value = float(value)
            return {"parameter": param.name, "value": float(param.value)}
        except Exception as e:
            self.log_message("Error setting device parameter: " + str(e))
            raise

    # ── Clip operations ───────────────────────────────────────────────────────

    def _delete_clip(self, track_index, clip_index):
        try:
            slot = self._get_clip_slot(track_index, clip_index, require_clip=True)
            slot.delete_clip()
            return {"deleted": True}
        except Exception as e:
            self.log_message("Error deleting clip: " + str(e))
            raise

    def _get_clip_notes(self, track_index, clip_index):
        try:
            clip = self._get_clip(track_index, clip_index)
            if not clip.is_midi_clip:
                raise Exception("Not a MIDI clip")
            notes = []
            try:
                for note in clip.get_all_notes_extended():
                    nd = {
                        "pitch": int(note.pitch),
                        "start_time": float(note.start_time),
                        "duration": float(note.duration),
                        "velocity": float(note.velocity),
                        "mute": bool(note.mute)
                    }
                    if hasattr(note, 'note_id'):
                        nd["note_id"] = int(note.note_id)
                    if hasattr(note, 'probability'):
                        nd["probability"] = float(note.probability)
                    if hasattr(note, 'velocity_deviation'):
                        nd["velocity_deviation"] = float(note.velocity_deviation)
                    notes.append(nd)
            except AttributeError:
                for note in clip.get_notes(0, 0, clip.length, 128):
                    notes.append({
                        "pitch": int(note[0]),
                        "start_time": float(note[1]),
                        "duration": float(note[2]),
                        "velocity": float(note[3]),
                        "mute": bool(note[4]) if len(note) > 4 else False
                    })
            return {"note_count": len(notes), "notes": notes}
        except Exception as e:
            self.log_message("Error getting clip notes: " + str(e))
            raise

    def _delete_notes_from_clip(self, track_index, clip_index, from_time, time_span, from_pitch, pitch_span):
        try:
            clip = self._get_clip(track_index, clip_index)
            if not clip.is_midi_clip:
                raise Exception("Not a MIDI clip")
            clip.remove_notes(float(from_time), int(from_pitch), float(time_span), int(pitch_span))
            return {"deleted": True}
        except Exception as e:
            self.log_message("Error deleting notes from clip: " + str(e))
            raise

    def _set_clip_loop(self, track_index, clip_index, looping, loop_start, loop_end):
        try:
            clip = self._get_clip(track_index, clip_index)
            clip.looping = bool(looping)
            if loop_start is not None:
                clip.loop_start = float(loop_start)
            if loop_end is not None:
                clip.loop_end = float(loop_end)
            return {
                "looping": clip.looping,
                "loop_start": clip.loop_start,
                "loop_end": clip.loop_end
            }
        except Exception as e:
            self.log_message("Error setting clip loop: " + str(e))
            raise

    def _set_clip_pitch(self, track_index, clip_index, coarse, fine):
        try:
            clip = self._get_clip(track_index, clip_index)
            if not clip.is_audio_clip:
                raise Exception("Pitch adjustment only applies to audio clips")
            if coarse is not None:
                clip.pitch_coarse = int(coarse)
            if fine is not None:
                clip.pitch_fine = float(fine)
            return {"pitch_coarse": clip.pitch_coarse, "pitch_fine": clip.pitch_fine}
        except Exception as e:
            self.log_message("Error setting clip pitch: " + str(e))
            raise

    def _set_clip_gain(self, track_index, clip_index, gain):
        try:
            clip = self._get_clip(track_index, clip_index)
            if not clip.is_audio_clip:
                raise Exception("Gain only applies to audio clips")
            clip.gain = float(gain)
            return {"gain": float(clip.gain)}
        except Exception as e:
            self.log_message("Error setting clip gain: " + str(e))
            raise

    def _set_clip_warp_mode(self, track_index, clip_index, warp_mode, warping):
        try:
            clip = self._get_clip(track_index, clip_index)
            if not clip.is_audio_clip:
                raise Exception("Warp mode only applies to audio clips")
            if warping is not None:
                clip.warping = bool(warping)
            if warp_mode is not None:
                clip.warp_mode = int(warp_mode)
            return {"warping": clip.warping, "warp_mode": clip.warp_mode}
        except Exception as e:
            self.log_message("Error setting clip warp mode: " + str(e))
            raise

    def _set_clip_signature(self, track_index, clip_index, numerator, denominator):
        try:
            clip = self._get_clip(track_index, clip_index)
            clip.signature_numerator = int(numerator)
            clip.signature_denominator = int(denominator)
            return {
                "signature_numerator": clip.signature_numerator,
                "signature_denominator": clip.signature_denominator
            }
        except Exception as e:
            self.log_message("Error setting clip signature: " + str(e))
            raise

    def _duplicate_clip_in_session(self, track_index, src_clip_index, dst_clip_index):
        try:
            track = self._get_track(track_index)
            src_slot = self._get_clip_slot(track_index, src_clip_index, require_clip=True)
            if dst_clip_index < 0 or dst_clip_index >= len(track.clip_slots):
                raise IndexError("Destination clip index out of range")
            dst_slot = track.clip_slots[dst_clip_index]
            if dst_slot.has_clip:
                raise Exception("Destination slot already has a clip")
            src_slot.duplicate_clip_to(dst_slot)
            return {"duplicated": True, "destination_index": dst_clip_index}
        except Exception as e:
            self.log_message("Error duplicating clip in session: " + str(e))
            raise

    # ── Scene operations ──────────────────────────────────────────────────────

    def _fire_scene(self, scene_index):
        try:
            if scene_index < 0 or scene_index >= len(self._song.scenes):
                raise IndexError("Scene index out of range")
            self._song.scenes[scene_index].fire()
            return {"fired": True, "scene_index": scene_index}
        except Exception as e:
            self.log_message("Error firing scene: " + str(e))
            raise

    def _create_scene(self, index):
        try:
            self._song.create_scene(index)
            new_index = len(self._song.scenes) - 1 if index == -1 else index
            return {"index": new_index, "name": self._song.scenes[new_index].name}
        except Exception as e:
            self.log_message("Error creating scene: " + str(e))
            raise

    def _delete_scene(self, scene_index):
        try:
            if scene_index < 0 or scene_index >= len(self._song.scenes):
                raise IndexError("Scene index out of range")
            self._song.delete_scene(scene_index)
            return {"deleted": True}
        except Exception as e:
            self.log_message("Error deleting scene: " + str(e))
            raise

    def _duplicate_scene(self, scene_index):
        try:
            if scene_index < 0 or scene_index >= len(self._song.scenes):
                raise IndexError("Scene index out of range")
            self._song.duplicate_scene(scene_index)
            return {"duplicated": True, "original_index": scene_index}
        except Exception as e:
            self.log_message("Error duplicating scene: " + str(e))
            raise

    def _set_scene_name(self, scene_index, name):
        try:
            if scene_index < 0 or scene_index >= len(self._song.scenes):
                raise IndexError("Scene index out of range")
            self._song.scenes[scene_index].name = name
            return {"name": self._song.scenes[scene_index].name}
        except Exception as e:
            self.log_message("Error setting scene name: " + str(e))
            raise

    def _set_scene_tempo(self, scene_index, tempo, enabled):
        try:
            if scene_index < 0 or scene_index >= len(self._song.scenes):
                raise IndexError("Scene index out of range")
            scene = self._song.scenes[scene_index]
            scene.tempo = float(tempo)
            scene.tempo_enabled = bool(enabled)
            return {"tempo": scene.tempo, "tempo_enabled": scene.tempo_enabled}
        except Exception as e:
            self.log_message("Error setting scene tempo: " + str(e))
            raise

    # ── Transport & recording ─────────────────────────────────────────────────

    def _set_time_signature(self, numerator, denominator):
        try:
            self._song.signature_numerator = int(numerator)
            self._song.signature_denominator = int(denominator)
            return {
                "signature_numerator": self._song.signature_numerator,
                "signature_denominator": self._song.signature_denominator
            }
        except Exception as e:
            self.log_message("Error setting time signature: " + str(e))
            raise

    def _set_loop_points(self, loop_on, loop_start, loop_length):
        try:
            self._song.loop = bool(loop_on)
            if loop_start is not None:
                self._song.loop_start = float(loop_start)
            if loop_length is not None:
                self._song.loop_length = float(loop_length)
            return {
                "loop": self._song.loop,
                "loop_start": self._song.loop_start,
                "loop_length": self._song.loop_length
            }
        except Exception as e:
            self.log_message("Error setting loop points: " + str(e))
            raise

    def _continue_playback(self):
        try:
            self._song.continue_playing()
            return {"playing": self._song.is_playing}
        except Exception as e:
            self.log_message("Error continuing playback: " + str(e))
            raise

    def _set_metronome(self, enabled):
        try:
            self._song.metronome = bool(enabled)
            return {"metronome": self._song.metronome}
        except Exception as e:
            self.log_message("Error setting metronome: " + str(e))
            raise

    def _set_record_mode(self, enabled):
        try:
            self._song.record_mode = bool(enabled)
            return {"record_mode": self._song.record_mode}
        except Exception as e:
            self.log_message("Error setting record mode: " + str(e))
            raise

    def _set_session_record(self, enabled):
        try:
            self._song.session_record = bool(enabled)
            return {"session_record": self._song.session_record}
        except Exception as e:
            self.log_message("Error setting session record: " + str(e))
            raise

    def _capture_midi(self):
        try:
            if not getattr(self._song, 'can_capture_midi', True):
                raise Exception("Cannot capture MIDI at this time")
            self._song.capture_midi()
            return {"captured": True}
        except Exception as e:
            self.log_message("Error capturing MIDI: " + str(e))
            raise

    def _undo(self):
        try:
            if not getattr(self._song, 'can_undo', True):
                raise Exception("Nothing to undo")
            self._song.undo()
            return {"undone": True}
        except Exception as e:
            self.log_message("Error undoing: " + str(e))
            raise

    def _redo(self):
        try:
            if not getattr(self._song, 'can_redo', True):
                raise Exception("Nothing to redo")
            self._song.redo()
            return {"redone": True}
        except Exception as e:
            self.log_message("Error redoing: " + str(e))
            raise

    def _tap_tempo(self):
        try:
            self._song.tap_tempo()
            return {"tempo": self._song.tempo}
        except Exception as e:
            self.log_message("Error tapping tempo: " + str(e))
            raise

    def _jump_to_cue(self, direction):
        try:
            if direction == "next":
                if not getattr(self._song, 'can_jump_to_next_cue', True):
                    raise Exception("No next cue point")
                self._song.jump_to_next_cue()
            else:
                if not getattr(self._song, 'can_jump_to_prev_cue', True):
                    raise Exception("No previous cue point")
                self._song.jump_to_prev_cue()
            return {"jumped": True, "current_song_time": self._song.current_song_time}
        except Exception as e:
            self.log_message("Error jumping to cue: " + str(e))
            raise

    def _stop_all_clips(self, quantized):
        try:
            self._song.stop_all_clips(bool(quantized))
            return {"stopped": True}
        except Exception as e:
            self.log_message("Error stopping all clips: " + str(e))
            raise

    # ── Routing ───────────────────────────────────────────────────────────────

    def _set_track_input_routing(self, track_index, routing):
        try:
            track = self._get_track(track_index)
            track.current_input_routing = routing
            return {"current_input_routing": track.current_input_routing}
        except Exception as e:
            self.log_message("Error setting track input routing: " + str(e))
            raise

    def _set_track_output_routing(self, track_index, routing):
        try:
            track = self._get_track(track_index)
            track.current_output_routing = routing
            return {"current_output_routing": track.current_output_routing}
        except Exception as e:
            self.log_message("Error setting track output routing: " + str(e))
            raise

    def _set_track_monitoring(self, track_index, monitoring_state):
        try:
            track = self._get_track(track_index)
            track.current_monitoring_state = int(monitoring_state)
            return {"current_monitoring_state": track.current_monitoring_state}
        except Exception as e:
            self.log_message("Error setting track monitoring: " + str(e))
            raise

    def _set_send_amount(self, track_index, send_index, value):
        try:
            track = self._get_track(track_index)
            sends = track.mixer_device.sends
            if send_index < 0 or send_index >= len(sends):
                raise IndexError("Send index out of range (track has " + str(len(sends)) + " sends)")
            sends[send_index].value = float(value)
            return {"send_index": send_index, "value": float(sends[send_index].value)}
        except Exception as e:
            self.log_message("Error setting send amount: " + str(e))
            raise

    # ── Master & return tracks ────────────────────────────────────────────────

    def _set_master_volume(self, volume):
        try:
            self._song.master_track.mixer_device.volume.value = float(volume)
            return {"volume": float(self._song.master_track.mixer_device.volume.value)}
        except Exception as e:
            self.log_message("Error setting master volume: " + str(e))
            raise

    def _set_master_pan(self, pan):
        try:
            self._song.master_track.mixer_device.panning.value = float(pan)
            return {"panning": float(self._song.master_track.mixer_device.panning.value)}
        except Exception as e:
            self.log_message("Error setting master pan: " + str(e))
            raise

    def _set_crossfader(self, value):
        try:
            self._song.master_track.mixer_device.crossfader.value = float(value)
            return {"crossfader": float(self._song.master_track.mixer_device.crossfader.value)}
        except Exception as e:
            self.log_message("Error setting crossfader: " + str(e))
            raise

    def _get_return_track_info(self, track_index):
        try:
            track = self._get_return_track(track_index)
            devices = []
            for i, d in enumerate(track.devices):
                devices.append({
                    "index": i,
                    "name": d.name,
                    "class_name": d.class_name,
                    "type": self._get_device_type(d)
                })
            return {
                "index": track_index,
                "name": track.name,
                "mute": track.mute,
                "solo": track.solo,
                "volume": float(track.mixer_device.volume.value),
                "panning": float(track.mixer_device.panning.value),
                "devices": devices
            }
        except Exception as e:
            self.log_message("Error getting return track info: " + str(e))
            raise

    def _set_return_track_name(self, track_index, name):
        try:
            track = self._get_return_track(track_index)
            track.name = name
            return {"name": track.name}
        except Exception as e:
            self.log_message("Error setting return track name: " + str(e))
            raise

    def _set_return_track_volume(self, track_index, volume):
        try:
            track = self._get_return_track(track_index)
            track.mixer_device.volume.value = float(volume)
            return {"volume": float(track.mixer_device.volume.value)}
        except Exception as e:
            self.log_message("Error setting return track volume: " + str(e))
            raise

    def _set_return_track_pan(self, track_index, pan):
        try:
            track = self._get_return_track(track_index)
            track.mixer_device.panning.value = float(pan)
            return {"panning": float(track.mixer_device.panning.value)}
        except Exception as e:
            self.log_message("Error setting return track pan: " + str(e))
            raise

    def _set_return_track_mute(self, track_index, mute):
        try:
            track = self._get_return_track(track_index)
            track.mute = bool(mute)
            return {"mute": track.mute}
        except Exception as e:
            self.log_message("Error setting return track mute: " + str(e))
            raise

    # ── Advanced ──────────────────────────────────────────────────────────────

    def _get_rack_chains(self, track_index, device_index):
        try:
            track = self._get_track(track_index)
            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            device = track.devices[device_index]
            if not getattr(device, 'can_have_chains', False):
                raise Exception("Device is not a rack")
            chains = []
            for i, chain in enumerate(device.chains):
                chain_devices = []
                for j, d in enumerate(chain.devices):
                    chain_devices.append({"index": j, "name": d.name, "class_name": d.class_name})
                chains.append({
                    "index": i,
                    "name": chain.name,
                    "mute": getattr(chain, 'mute', False),
                    "solo": getattr(chain, 'solo', False),
                    "devices": chain_devices
                })
            return {"chain_count": len(chains), "chains": chains}
        except Exception as e:
            self.log_message("Error getting rack chains: " + str(e))
            raise

    def _set_rack_macro(self, track_index, device_index, macro_index, value):
        try:
            track = self._get_track(track_index)
            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            device = track.devices[device_index]
            if not getattr(device, 'can_have_chains', False):
                raise Exception("Device is not a rack")
            macros = [p for p in device.parameters if "Macro" in p.name]
            if macro_index < 0 or macro_index >= len(macros):
                raise IndexError("Macro index out of range (rack has " + str(len(macros)) + " macros)")
            macros[macro_index].value = float(value)
            return {"macro_name": macros[macro_index].name, "value": float(macros[macro_index].value)}
        except Exception as e:
            self.log_message("Error setting rack macro: " + str(e))
            raise

    def _get_plugin_presets(self, track_index, device_index):
        try:
            track = self._get_track(track_index)
            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            device = track.devices[device_index]
            if not hasattr(device, 'presets'):
                raise Exception("Device does not have presets (not a plugin)")
            presets = list(device.presets)
            selected = getattr(device, 'selected_preset_index', -1)
            return {"preset_count": len(presets), "presets": presets, "selected_preset_index": selected}
        except Exception as e:
            self.log_message("Error getting plugin presets: " + str(e))
            raise

    def _set_plugin_preset(self, track_index, device_index, preset_index):
        try:
            track = self._get_track(track_index)
            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            device = track.devices[device_index]
            if not hasattr(device, 'selected_preset_index'):
                raise Exception("Device does not support preset selection")
            device.selected_preset_index = int(preset_index)
            return {"selected_preset_index": device.selected_preset_index}
        except Exception as e:
            self.log_message("Error setting plugin preset: " + str(e))
            raise

    def _set_song_scale(self, root_note, scale_name, scale_mode):
        try:
            if not hasattr(self._song, 'root_note'):
                raise Exception("Song key/scale requires Live 12+")
            result = {}
            if root_note is not None:
                self._song.root_note = int(root_note)
                result["root_note"] = self._song.root_note
            if scale_name is not None:
                self._song.scale_name = scale_name
                result["scale_name"] = self._song.scale_name
            if scale_mode is not None:
                self._song.scale_mode = bool(scale_mode)
                result["scale_mode"] = self._song.scale_mode
            return result
        except Exception as e:
            self.log_message("Error setting song scale: " + str(e))
            raise

    def get_browser_items_at_path(self, path):
        """
        Get browser items at a specific path.
        
        Args:
            path: Path in the format "category/folder/subfolder"
                 where category is one of: instruments, sounds, drums, audio_effects, midi_effects
                 or any other available browser category
                 
        Returns:
            Dictionary with items at the specified path
        """
        try:
            # Access the application's browser instance instead of creating a new one
            app = self.application()
            if not app:
                raise RuntimeError("Could not access Live application")
                
            # Check if browser is available
            if not hasattr(app, 'browser') or app.browser is None:
                raise RuntimeError("Browser is not available in the Live application")
            
            # Log available browser attributes to help diagnose issues
            browser_attrs = [attr for attr in dir(app.browser) if not attr.startswith('_')]
            self.log_message("Available browser attributes: {0}".format(browser_attrs))
                
            # Parse the path
            path_parts = path.split("/")
            if not path_parts:
                raise ValueError("Invalid path")
            
            # Determine the root category
            root_category = path_parts[0].lower()
            current_item = None
            
            # Check standard categories first
            if root_category == "instruments" and hasattr(app.browser, 'instruments'):
                current_item = app.browser.instruments
            elif root_category == "sounds" and hasattr(app.browser, 'sounds'):
                current_item = app.browser.sounds
            elif root_category == "drums" and hasattr(app.browser, 'drums'):
                current_item = app.browser.drums
            elif root_category == "audio_effects" and hasattr(app.browser, 'audio_effects'):
                current_item = app.browser.audio_effects
            elif root_category == "midi_effects" and hasattr(app.browser, 'midi_effects'):
                current_item = app.browser.midi_effects
            else:
                # Try to find the category in other browser attributes
                found = False
                for attr in browser_attrs:
                    if attr.lower() == root_category:
                        try:
                            current_item = getattr(app.browser, attr)
                            found = True
                            break
                        except Exception as e:
                            self.log_message("Error accessing browser attribute {0}: {1}".format(attr, str(e)))
                
                if not found:
                    # If we still haven't found the category, return available categories
                    return {
                        "path": path,
                        "error": "Unknown or unavailable category: {0}".format(root_category),
                        "available_categories": browser_attrs,
                        "items": []
                    }
            
            # Navigate through the path
            for i in range(1, len(path_parts)):
                part = path_parts[i]
                if not part:  # Skip empty parts
                    continue
                
                if not hasattr(current_item, 'children'):
                    return {
                        "path": path,
                        "error": "Item at '{0}' has no children".format('/'.join(path_parts[:i])),
                        "items": []
                    }
                
                found = False
                for child in current_item.children:
                    if hasattr(child, 'name') and child.name.lower() == part.lower():
                        current_item = child
                        found = True
                        break
                
                if not found:
                    return {
                        "path": path,
                        "error": "Path part '{0}' not found".format(part),
                        "items": []
                    }
            
            # Get items at the current path
            items = []
            if hasattr(current_item, 'children'):
                for child in current_item.children:
                    item_info = {
                        "name": child.name if hasattr(child, 'name') else "Unknown",
                        "is_folder": hasattr(child, 'children') and bool(child.children),
                        "is_device": hasattr(child, 'is_device') and child.is_device,
                        "is_loadable": hasattr(child, 'is_loadable') and child.is_loadable,
                        "uri": child.uri if hasattr(child, 'uri') else None
                    }
                    items.append(item_info)
            
            result = {
                "path": path,
                "name": current_item.name if hasattr(current_item, 'name') else "Unknown",
                "uri": current_item.uri if hasattr(current_item, 'uri') else None,
                "is_folder": hasattr(current_item, 'children') and bool(current_item.children),
                "is_device": hasattr(current_item, 'is_device') and current_item.is_device,
                "is_loadable": hasattr(current_item, 'is_loadable') and current_item.is_loadable,
                "items": items
            }
            
            self.log_message("Retrieved {0} items at path: {1}".format(len(items), path))
            return result
            
        except Exception as e:
            self.log_message("Error getting browser items at path: {0}".format(str(e)))
            self.log_message(traceback.format_exc())
            raise
