require 'securerandom'
require 'socket'
require 'timeout'

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
    servers = ADDRS.sample 2
    results = servers.map do |host, port|
      test host, port
    end
    if results.uniq.size != 1
      p servers.zip(results)
      raise 'Symmetric NAT. unsupported'
    end
    results[0]
  end

  def test(host, port, legacy = false)
    magic_cookie = 0x2112A442
    magic_cookie = rand(0xffffffff) if legacy
    magic_cookie_transaction_id = [magic_cookie].pack('N') + SecureRandom.random_bytes(12)
    @socket.send [1, 0].pack('nn') + magic_cookie_transaction_id, 0, host, port
    data = Timeout.timeout 5 do
      loop do
        response, _addr = @socket.recvfrom 1024
        break response if magic_cookie_transaction_id == response[4...20]
      end
    end
    attr_chars = data[20..].chars
    attrs = {}
    until attr_chars.empty?
      type, size = attr_chars.shift(4).join.unpack('nn')
      value = attr_chars.shift((size + 3) / 4 * 4).take(size).join
      attrs[type] = value
    end
    # 1: MAPPED-ADDRESS, 2: XOR-MAPPED-ADDRESS
    value = attrs[1] || attrs[32]
    return unless value
    xor = attrs[1] ? [0] : magic_cookie_transaction_id.unpack('N*')
    family, xport, *xip = value.unpack 'nnN*'
    port = xport ^ (xor[0] >> 16)
    ip = xip.zip(xor.cycle).map { _1 ^ _2 }.pack('N*').unpack 'C*'
    case family
    when 1 # IPv4
      [ip.join('.'), port]
    when 2 # IPv6
      [ip.join(':'), port]
    end
  rescue Timeout::Error
    nil
  end
end
