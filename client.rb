require_relative 'stun'
require_relative 'connection'
socket = UDPSocket.open
# stun = Stun.new socket
# global_ip, global_port = stun.get_ip_port
# p [global_ip, global_port]

local_port = socket.addr[1]
server = ConnectionManager.new socket

loop do
  server.accept
end
