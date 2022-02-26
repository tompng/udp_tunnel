require 'socket'
require_relative 'connection'
require_relative 'usocket'
socket = UDPSocket.open
socket.bind '0.0.0.0', 0

local_port = socket.addr[1]
p local_port
manager = ConnectionManager.new socket

def pipe_socket(from, to)
  Thread.new do
    loop do
      data = from.readpartial 1024
      break if data.nil? || data.empty?
      to.write data
      to.flush
    end
  rescue
  ensure
    from.close rescue nil
    to.close rescue nil
  end
end

def start_server(manager)
  server = Server.new manager
  loop do
    socket = server.accept
    p :accepted
    tcpsocket = TCPSocket.new 'localhost', 8080
    pipe_socket socket, tcpsocket
    pipe_socket tcpsocket, socket
  end
end

def start_client(manager, ip, port)
  connection = manager.connect ip, port
  client = Client.new connection
  server = TCPServer.new 8081
  loop do
    tcpsocket = server.accept
    socket = client.connect
    pipe_socket socket, tcpsocket
    pipe_socket tcpsocket, socket
  end
end

port_file = 'port.txt'
case ARGV[0]
when 'server'
  File.write port_file, local_port.to_s
  at_exit { File.unlink port_file }
  start_server manager
when 'client'
  sleep 0.1 until File.exist? port_file
  port = File.read(port_file).to_i
  start_client manager, '127.0.0.1', port
end
