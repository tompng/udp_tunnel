require 'socket'
require 'reline'
require 'optparse'
require_relative 'connection'
require_relative 'usocket'
require_relative 'stun'

bind_addr = '0.0.0.0'
server_addr = nil

opt = OptionParser.new
opt.on('-b', '--bind IP', 'bind ip. `0.0.0.0` or `::`') { bind_addr = _1 }
opt.on('-ipv4') { bind_addr = '0.0.0.0' }
opt.on('-ipv6') { bind_addr = '::' }
opt.on('-s', '--server ADDR', 'Accepts udp connection and proxy to server. ex: `127.0.0.1:3000`') { server_addr = _1 }
opt.parse ARGV
socket = UDPSocket.open bind_addr.include?(':') ? Socket::AF_INET6 : Socket::AF_INET
socket.bind bind_addr, 0

local_udp_port = socket.addr[1]
puts "local udp port: #{local_udp_port}"
class Runner
  def initialize(socket)
    @manager = ConnectionManager.new socket
    # @manager.emulate_packet_loss = 0.1
    # @manager.emulate_packet_delay = 0.2
    @clients = {}
    @cnt = 0
    @manager.on_connect = -> connection { p [:udp_connected, connection.ip, connection.port] }
    @manager.on_close = -> connection { p [:udp_closed, connection.ip, connection.port]}
  end

  def pipe_socket(from, to, onclose = nil)
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
      onclose&.call
    end
  end

  def run_server(addr)
    server = Server.new @manager
    raise "port not specified: #{addr}" unless /^(?<host>.+):(?<port>\d+)$/ =~ addr
    loop do
      socket = server.accept
      id = @cnt += 1
      info = [socket.connection.ip, socket.connection.port]
      p [:accept, id, info]
      tcpsocket = TCPSocket.new host, port.to_i
      pipe_socket socket, tcpsocket
      pipe_socket tcpsocket, socket, -> { p [:closed, id, info] }
    end
  end

  def run_client(tcp_server, ip, port)
    client = @clients[[ip, port]] ||= Client.new @manager.connect(ip, port)
    loop do
      tcp_socket = tcp_server.accept
      socket = client.connect
      id = @cnt += 1
      info = { from: tcp_server.addr[1], to: [ip, port] }
      p [:connect, id, info]
      pipe_socket socket, tcp_socket
      pipe_socket tcp_socket, socket, -> { p [:close, id, info] }
    end
  end

  def accept(ip, port)
    @manager.connect ip, port, mark_for_accept: true
  end
end

runner = Runner.new socket

if server_addr
  Thread.new do
    runner.run_server server_addr
  end
end

Reline.autocompletion = true
commands = %w[connect show]
commands << 'accept' if server_addr
Reline.completion_proc = lambda do |target, pre = nil|
  commands.map { _1 + ' ' }.select { _1.start_with? target } if target == pre
end

begin
  loop do
    text = Reline.readline('> ', true)
    case text
    when /^connect/
      if /^connect +(?<local_port>\d+) +-> +(?<remote_ip>\S+) +(?<remote_port>\d+)/ =~ text
        tcp_server = TCPServer.new local_port.to_i
        Thread.new { runner.run_client tcp_server, remote_ip, remote_port.to_i }
      else
        puts <<~MESSAGE
          Invalid format. `connect [local_port] -> [remote_ip] [remote_port]`
          example: `connect 8080 -> 1.2.3.4 5678`
        MESSAGE
      end
    when /^accept/
      if !server_addr
        puts 'cannot use if --server is not provided'
      elsif /^accept +(?<remote_ip>\S+) +(?<remote_port>\d+)/ =~ text
        runner.accept remote_ip, remote_port.to_i
      else
        puts <<~MESSAGE
          Invalid format. `accept [remote_ip] [remote_port]`
          example: `accept 1.2.3.4 5678`
        MESSAGE
      end
    when /^show/
      p Stun.get_ip_port socket
    when /^exit/
      break
    else
      unless text.strip.empty?
        puts <<~MESSAGE
          command not found: #{text}
          > connect [local_port] -> [remote_ip] [remote_port]
          > accept [remote_ip] [remote_port]
          > show
          > exit
        MESSAGE
      end
    end
  rescue Interrupt
    exit
  rescue => e
    p e
  end
end
