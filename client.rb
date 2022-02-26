require_relative 'stun'
require_relative 'connection'
socket = UDPSocket.open
socket.bind '0.0.0.0', 0
# stun = Stun.new socket
# global_ip, global_port = stun.get_ip_port
# p [global_ip, global_port]

local_port = socket.addr[1]
p local_port

server = ConnectionManager.new socket

connections = []
conn = nil
Thread.new do
  loop do
    conn = server.accept
    p :accepted
    connections << conn
    Thread.new {
      loop do
        type, data = conn.read
        p [type, data]
        if data&.start_with? 'echo'
          conn.send_data 'ok' + data[4..]
        end
        if type == :close
          connections -= [conn]
          break
        end
      end
    }
  end
end

port_file = 'port.txt'
case ARGV[0]
when 'a'
  File.write port_file, local_port.to_s
  at_exit { File.unlink port_file }
when 'b'
  sleep 0.1 until File.exist? port_file
  port = File.read(port_file).to_i
  conn = server.connect '127.0.0.1', port
else
  define_singleton_method(:connect){ server.connect '127.0.0.1', _1 }
end

binding.irb
