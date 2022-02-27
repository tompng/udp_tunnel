require 'securerandom'
require 'socket'
require 'timeout'

module Stun
  ADDRS = [
    ['stun.l.google.com', 19302],
    ['stun1.l.google.com', 19302],
    ['stun2.l.google.com', 19302],
    ['stun3.l.google.com', 19302],
    ['stun4.l.google.com', 19302]
  ]

  class Error < StandardError; end

  MAGIC_COOKIE = 0x2112A442

  def self.request
    magic = [MAGIC_COOKIE].pack('N') + SecureRandom.random_bytes(12)
    [1, 0].pack('nn') + magic
  end

  def self.response?(response, request: nil)
    return false unless response.unpack('CC') == [1, 1]
    if request
      response[4, 16] == request[4...20]
    else
      response[4, 4].unpack1('N') == MAGIC_COOKIE
    end
  end

  def self.parse_response(response)
    magic_cookie_transaction_id = response[4...20]
    attr_chars = response[20..].chars
    attrs = {}
    until attr_chars.empty?
      type, size = attr_chars.shift(4).join.unpack('nn')
      value = attr_chars.shift((size + 3) / 4 * 4).take(size).join
      attrs[type] = value
    end
    # 1: MAPPED-ADDRESS, 2: XOR-MAPPED-ADDRESS
    value = attrs[1] || attrs[32]
    raise Error, "Packet Parse Error #{response}" unless value
    xor = attrs[1] ? [0] : magic_cookie_transaction_id.unpack('N*')
    family, xport, *xip = value.unpack 'nnN*'
    port = xport ^ (xor[0] >> 16)
    ip = xip.zip(xor.cycle).map { _1 ^ _2 }.pack('N*').unpack 'C*'
    case family
    when 1 # IPv4
      [ip.join('.'), port]
    when 2 # IPv6
      [ip.join(':'), port]
    else
      raise Error, "Packet Parse Error #{response}"
    end
  end
end
