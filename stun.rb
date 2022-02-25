require 'securerandom'
require 'socket'

class Stun
  def initialize(socket)
    @socket = socket
  end

  ADDRS = [
    ['stun.l.google.com', 19302],
    ['stun1.l.google.com', 19302],
    ['stun2.l.google.com', 19302],
    ['stun3.l.google.com', 19302],
    ['stun4.l.google.com', 19302]
  ]

  def get_ip_port
    results = ADDRS.sample(2).map do |host, port|
      test host, port
    end
    if results.uniq.size != 1
      raise 'Symmetric NAT. unsupported'
    end
    results[0]
  end

  def test(host, port)
    id = SecureRandom.random_bytes 16
    @socket.send [1, 0].pack('nn') + id, 0, host, port
    data, _addr = @socket.recvfrom 1024
    raise 'invalid response from stun server' unless id == data[4...20]
    attr_chars = data[20..].chars
    attrs = {}
    until attr_chars.empty?
      type, size = attr_chars.shift(4).join.unpack('nn')
      value = attr_chars.shift(size).join
      attrs[type] = value
    end
    value = attrs[1]
    global_port = value[2, 2].unpack1 'n'
    global_ip = value[4..].bytes
    [global_ip, global_port]
  end
end
