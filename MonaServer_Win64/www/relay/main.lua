--[[
  MonaServer2 WebSocket relay for FantasyCrest

  URL: ws://<host>:19350/relay

  Client -> server:
    ["hello", "<roomId>"]        host registers a room
    ["join",  "<roomId>"]        guest joins a room
    ["data",  "<payload>"]       opaque game payload, forwarded to the peer
    ["ping",  <timestamp>]       keepalive / RTT probe

  Server -> client:
    ["welcome",   "<clientId>"]
    ["hello_ok",  "<roomId>"]
    ["join_ok",   "<roomId>"]
    ["peer_join", "<clientId>"]
    ["peer_leave","<reason>"]
    ["data",      "<payload>"]
    ["pong",      <timestamp>]
    ["error",     "<reason>"]
]]

local rooms = {}   -- roomId -> { host = client, guest = client }
local members = {} -- client -> { room = roomId, role = "host"|"guest" }

local MAX_ROOM_ID = 64
local MAX_PAYLOAD = 65536

local function send(client, name, ...)
   if not client then
      return
   end
   client:writeInvocation(name, ...)
   client:flush()
end

local function clientId(client)
   return tostring(client.address or "?")
end

local function validRoom(room)
   return type(room) == "string" and #room > 0 and #room <= MAX_ROOM_ID
end

local function detach(client)
   local member = members[client]
   if not member then
      return
   end

   local room = rooms[member.room]
   if room then
      if member.role == "host" then
         rooms[member.room] = nil
         if room.guest then
            members[room.guest] = nil
            send(room.guest, "peer_leave", "host closed")
         end
      else
         room.guest = nil
         send(room.host, "peer_leave", "guest closed")
      end
   end

   members[client] = nil
   INFO("[relay] leave " .. clientId(client) .. " role=" .. tostring(member.role))
end

function onStart()
   rooms = {}
   members = {}
   INFO("[relay] onStart")
end

function onStop()
   INFO("[relay] onStop")
   rooms = {}
   members = {}
end

function onConnection(client, params)
   INFO("[relay] connect " .. clientId(client))
   send(client, "welcome", clientId(client))

   client.hello = function(self, room)
      if members[self] then
         send(self, "error", "already in room")
         return
      end
      if not validRoom(room) then
         send(self, "error", "invalid room")
         return
      end
      if rooms[room] and rooms[room].host then
         send(self, "error", "room exists")
         return
      end

      rooms[room] = { host = self }
      members[self] = { room = room, role = "host" }
      INFO("[relay] hello " .. clientId(self) .. " room=" .. room)
      send(self, "hello_ok", room)
   end

   client.join = function(self, room)
      if members[self] then
         send(self, "error", "already in room")
         return
      end
      if not validRoom(room) then
         send(self, "error", "invalid room")
         return
      end

      local roomData = rooms[room]
      if not roomData or not roomData.host then
         INFO("[relay] join failed " .. clientId(self) .. " room=" .. room .. " not found")
         send(self, "error", "room not found")
         return
      end
      if roomData.guest then
         INFO("[relay] join failed " .. clientId(self) .. " room=" .. room .. " full")
         send(self, "error", "room full")
         return
      end

      roomData.guest = self
      members[self] = { room = room, role = "guest" }
      INFO("[relay] join " .. clientId(self) .. " room=" .. room)
      send(self, "join_ok", room)
      send(roomData.host, "peer_join", clientId(self))
      INFO("[relay] peer_join " .. clientId(self) .. " -> " .. clientId(roomData.host))
   end

   client.data = function(self, payload)
      local member = members[self]
      if not member then
         send(self, "error", "not in room")
         return
      end
      local payloadType = type(payload)
      if payloadType ~= "string" and payloadType ~= "table" then
         return
      end
      if payloadType == "string" and #payload > MAX_PAYLOAD then
         send(self, "error", "payload too large")
         return
      end

      local roomData = rooms[member.room]
      if not roomData then
         return
      end
      local target
      if member.role == "host" then
         target = roomData.guest
      else
         target = roomData.host
      end
      if payloadType == "table" then
         local extra = ""
         if type(payload.a) == "table" and type(payload.a[1]) == "table" and type(payload.a[1].world) == "table" then
            extra = " world=" .. tostring(#payload.a[1].world)
         end
         INFO("[relay] data " .. clientId(self) .. " -> " .. (target and clientId(target) or "nil") .. " type=table method=" .. tostring(payload.n or "?") .. extra)
      else
         INFO("[relay] data " .. clientId(self) .. " -> " .. (target and clientId(target) or "nil") .. " type=string bytes=" .. tostring(#payload) .. " payload=" .. string.sub(payload,1,120))
      end
      send(target, "data", payload)
   end

   client.ping = function(self, timestamp)
      send(self, "pong", timestamp)
   end
end

function onDisconnection(client)
   detach(client)
end
