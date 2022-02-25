class ConnectionManager
  def initialize(socket)
    @socket = socket
    @connections = {}
  end

  TYPE_REQ = 0
  TYPE_ACK = 1
  TYPE_DATA = 2
  TYPE_RESEND = 3

  def run_recv
    loop do
      data, addr = @socket.recvfrom 65536
      ip = addr[3]
      port = addr[1]
      type = data[0]
      connection_id = data[1, 4]
      key = [ip, port, connection_id]
      msg = data[4..]
      case type
      when TYPE_REQ
        @socket.send TYPE_ACK.chr, 0, ip, port
      when TYPE_ACK
        ridx, scnt = msg.unpack1('NN')
        conn = @connections[key]
        if !conn && ridx == 0 && scnt == 0
          conn = @connections[key] = Connection.new(socket, self, ip, port, connection_id)
        end
        conn&.ack ridx, scnt
      when TYPE_DATA
        @connections[key]&.data(msg.unpack1('N'), msg[4..])
      when TYPE_RESEND
        @connections[key]&.resend(msg.unpack1('N*'))
      when TYPE_CLOSE
        @connections[key]&.trigger_close
        @connections.delete key
      end
    end
  end

  def connect(ip, port)

  end

  def run

  end
end

class Connection
  def initialize(socket, manager, ip, port, id)
    @manager = manager
    @ip = ip
    @port = port
    @connection_id = id
    @socket = socket
    @send_idx = 0
    @recv_idx = 0
    @send_buffer = []
    @recv_buffer = []
    @messages = Thread::Queue.new
    @unreceiveds = {}
    @state = nil
    @last_ack = Time.now
  end

  def close
    return if closed?
    trigger_close
    @manager.delete [@ip, @port, @connection_id]
  end

  def trigger_close
    return if closed?
    @state = :closed
    @messages << nil
  end

  def trigger_message(message)
    @messages << message
  end

  def read
    @messages.deq
  end

  def send_data(message)
    message.each_slice 1024 do |msg|
      idx = @send_idx + @send_buffer.size
      @send_buffer << msg
      socket_send ConnectionManager::TYPE_DATA, [idx].pack('N') + msg
    end
  end

  def closed?
    @state == :closed
  end

  def send_close
    return if closed?
    socket_send ConnectionManager::TYPE_CLOSE
  end

  def send_ack
    socket_send ConnectionManager::TYPE_ACK, [@recv_idx, @send_idx + @send_buffer.size].pack('NN')
  end

  def ack(reached_idx, sent_count)
    @send_buffer.shift reached_idx - @send_idx if reached_idx > @send_idx
    @send_idx = reached_idx
    @recv_buffer[sent_count - 1 - @recv_idx] ||= nil if sent_count > 0
    @last_ack = Time.now
    request_resend
  end

  def data(idx, msg)
    @recv_buffer[idx - @recv_idx] = msg
    @unreceiveds.delete idx
    while @recv_buffer.first
      @recv_idx += 1
      trigger_message msg @recv_buffer.shift
    end
    request_resend
  end

  def resend(idxs)
    idxs.each do |idx|
      msg = @send_buffer[idx - @send_idx]
      socket_send ConnectionManager::TYPE_DATA, [idx].pack('N') + msg
    end
  end

  def request_resend
    current = Time.now
    @recv_buffer.each.with_index @recv_idx do |msg, id|
      next if msg
      @unreceiveds[id] ||= current
    end
    timeout = 200
    ids = @unreceiveds.keys.select do |id|
      current - @unreceiveds[id] > timeout
    end
    ids.each { @unreceiveds[_1] = current + timeout }
    socket_send ConnectionManager::TYPE_RESEND, ids.pack('N*')
  end

  def socket_send(type, data = '')
    @socket.send type.chr + @connection_id + data, 0, @ip, @port
  end
end