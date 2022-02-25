require_relative 'stun'
socket = UDPSocket.open
stun = Stun.new socket
global_ip, global_port = stun.get_ip_port
p [global_ip, global_port]



